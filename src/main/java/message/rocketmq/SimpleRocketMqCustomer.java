package message.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

import java.nio.charset.StandardCharsets;


/**
 * 消费者负载均衡是在客户端做的，（类似于分页分配，每个消费者是页数，队列是size，最后求得消费者消费哪些个队列）
 * 一个消息队列在同一时间只允许被同一消费组内的一个消费者消费，一个消息消费者能同时消费多个消息队列，独占的
 *
 * Tag过滤，是服务器端过滤，是通过hash值过滤的，因此消费者拿到后最好再经过字符串比对最好
 * SQL92过滤，是客户端过滤
 */

/**
 * 消息重试消费机制：
 * RocketMQ会针对每个消费组有一个%RETRY%+consumerGroup重试队列
 * 各种异常原因导致消费者端无法消费的消息，会先保存至延迟队列，通过自带的延迟机制来做到重试级别越大间隔越久，之后到达时间将消息重新保存至%RETRY%+consumerGroup重试队列
 * 消费一直失败达到设定的最大次数后会发到消费者对应的特殊队列，叫做死信队列
 */
public class SimpleRocketMqCustomer {

    public static void main(String[] args) throws MQClientException {
        // 默认推送模式消费者，createOrder消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ORDER_CREATE");
        // 设置NameSrv地址，用来路由寻找Broker
        consumer.setNamesrvAddr("localhost:9876");
        // 设置订阅Topic与Tag，tag为*代表所有的不过滤
        consumer.subscribe("ORDER", "createOrder");
        // 注册消息监听者，用来处理从broker拉取过来的消息，并发处理
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach((msg) -> {
                String msgBody = new String(msg.getBody(), StandardCharsets.UTF_8);
                System.out.printf("%s 收到消息： %s %n", Thread.currentThread().getName() , msgBody);
            });

            // ack该消息已经消费成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 返回结果和重试机制关系，具体见ConsumeMessageConcurrentlyService.java:167

        });
        // 启动consumer实例，建立NameSrv与Broker连接
        consumer.start();

        // 最后关闭消费者
//        consumer.shutdown();

        //
    }
}
