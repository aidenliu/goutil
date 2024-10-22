package rabbitMQ

import (
	"fmt"
	"github.com/aidenliu/goutil/config"
	"github.com/streadway/amqp"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	// 连接失败后，重试时间间隔
	reconnectFailureDelay = 5 * time.Second
	// 连接断开后，重试时间间隔
	reconnectCloseDelay = 5 * time.Second
	// 等待连接就绪时间间隔
	connectWaitDelay = 5 * time.Second
)

type ConsumeResult struct {
	error
	Requeue bool
}

type RabbitConfig struct {
	ConfigKey string
	DialStr   []string
}

type RabbitMQ struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	dialStr     string
	connNotify  chan *amqp.Error
	chNotify    chan *amqp.Error
	done        chan struct{}
	isConnected bool
}

// ExchangeConfig 交换器配置
type ExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]interface{}
}

// QueueConfig 队列定义
type QueueConfig struct {
	Name       string
	RoutingKey string
	Durable    bool
	AutoDelete bool
	exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

// PublishConfig 生产者配置
type PublishConfig struct {
	ExChangeName string
	RoutingKey   string
	Mandatory    bool
	Immediate    bool
}

// ConsumeConfig 消费者配置
type ConsumeConfig struct {
	ConsumeQueue string
	AutoAck      bool
	Exclusive    bool
	NoLocal      bool
	NoWait       bool
	Args         map[string]interface{}
}

// New 创建RabbitMQ连接
func New(rc *RabbitConfig) (*RabbitMQ, error) {
	var err error
	r := &RabbitMQ{}
	if rc.ConfigKey != "" {
		configItem := config.Service(rc.ConfigKey)
		if configItem == nil {
			return nil, fmt.Errorf("rabbitMQ configKey %s not found", rc.ConfigKey)
		}
		r.dialStr = fmt.Sprintf("amqp://%s:%s@%s/", configItem["login"], configItem["password"], configItem["host"])
		if _, err = r.connect(); err != nil {
			return nil, err
		}
	} else {
		var connectSuc bool
		for _, v := range rc.DialStr {
			if v == "" {
				continue
			}
			r.dialStr = v
			if _, err = r.connect(); err == nil {
				connectSuc = true
				break
			}
		}
		if !connectSuc {
			return nil, fmt.Errorf("all rabbitMQ config connect failure:%s", strings.Join(rc.DialStr, ","))
		}
	}
	go r.reconnect()
	return r, nil
}

// reconnect 重连
func (r *RabbitMQ) reconnect() {
	for {
		if !r.isConnected {
			for {
				if _, err := r.connect(); err != nil {
					time.Sleep(reconnectFailureDelay)
				} else {
					break
				}
			}
		}
		// 监听关闭事件
		select {
		case <-r.done:
			return
		case <-r.connNotify:
			r.isConnected = false
		case <-r.chNotify:
			r.isConnected = false
		default:
			time.Sleep(reconnectCloseDelay)
		}
	}
}

// connect 建立连接
func (r *RabbitMQ) connect() (bool, error) {
	conn, err := amqp.Dial(r.dialStr)
	if err != nil {
		return false, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return false, err
	}
	r.isConnected = true
	r.connection = conn
	r.channel = ch
	// 注册连接/channel关闭事件，用于通知进行重连
	r.connNotify = make(chan *amqp.Error, 1)
	r.connection.NotifyClose(r.connNotify)
	r.chNotify = make(chan *amqp.Error, 1)
	r.channel.NotifyClose(r.chNotify)
	return true, nil
}

// Close 关闭连接
func (r *RabbitMQ) Close() {
	if r.isConnected {
		r.channel.Close()
		r.connection.Close()
		r.isConnected = false
	}
	close(r.done)
}

// Publish 发布消息
func (r *RabbitMQ) Publish(playLoad []byte, p *PublishConfig) error {
	err := r.channel.Publish(
		p.ExChangeName,
		p.RoutingKey,
		p.Mandatory,
		p.Immediate,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/json",
			Body:         playLoad,
		},
	)
	return err
}

// Consume 消费消息
func (r *RabbitMQ) Consume(consumerCount int, callBack func([]byte) ConsumeResult, c *ConsumeConfig) error {
	for {
		if !r.isConnected {
			log.Println("connect retry....")
			time.Sleep(connectWaitDelay)
			continue
		}
		r.channel.Qos(1, 0, false)
		deliveries := make([]<-chan amqp.Delivery, 0)
		for n := 0; n < consumerCount; n++ {
			if delivery, err := r.channel.Consume(
				c.ConsumeQueue,
				"",
				c.AutoAck,
				c.Exclusive,
				c.NoLocal,
				c.NoWait,
				c.Args,
			); err == nil {
				deliveries = append(deliveries, delivery)
			}
		}
		var wg sync.WaitGroup
		wg.Add(len(deliveries))
		for _, delivery := range deliveries {
			go func(delivery <-chan amqp.Delivery) {
				defer wg.Done()
				if delivery == nil {
					return
				}
				for d := range delivery {
					consumeResult := callBack(d.Body)
					if c.AutoAck == false {
						var ackErr error
						if consumeResult.error != nil {
							log.Println("callBack err:", consumeResult.error)
							switch consumeResult.Requeue {
							case false:
								ackErr = d.Nack(false, false)
							case true:
								ackErr = d.Nack(false, true)
							default:
								ackErr = d.Ack(false)
							}
						} else {
							ackErr = d.Ack(false)
						}
						if ackErr != nil {
							log.Println("ack err:", ackErr)
						}
					}
				}
				log.Println("delivery close...")
			}(delivery)
		}
		wg.Wait()
	}
	return nil
}

// InitQueue 创建exchange、queue 绑定队列到exchange
func (r *RabbitMQ) InitQueue(ex *ExchangeConfig, q *QueueConfig) (queue amqp.Queue, err error) {
	err = r.exchangeDeclare(ex)
	if err != nil {
		log.Println("exchangeDeclare error:", err, ex)
	}
	queue, err = r.queueDeclare(q)
	if err != nil {
		log.Println("queueDeclare error:", err, q)
	}
	err = r.queueBind(ex, q)
	if err != nil {
		log.Println("queueBind error:", err, ex, q)
	}
	return
}

// exchangeDeclare 定义Exchange
func (r *RabbitMQ) exchangeDeclare(ex *ExchangeConfig) error {
	err := r.channel.ExchangeDeclare(
		ex.Name,
		ex.Type,
		ex.Durable,
		ex.AutoDelete,
		ex.Internal,
		ex.NoWait,
		ex.Args,
	)
	return err
}

// 定义Queue
func (r *RabbitMQ) queueDeclare(q *QueueConfig) (amqp.Queue, error) {
	queue, err := r.channel.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.exclusive,
		q.NoWait,
		q.Args,
	)
	return queue, err
}

// 绑定队列
func (r *RabbitMQ) queueBind(ex *ExchangeConfig, q *QueueConfig) error {
	queueName := q.Name
	routingKey := q.RoutingKey
	exchangeName := ex.Name
	return r.channel.QueueBind(queueName, routingKey, exchangeName, false, nil)
}
