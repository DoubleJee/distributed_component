package message.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

import java.nio.charset.StandardCharsets;


public class SimpleRocketMqCustomer {

    public static void main(String[] args) throws MQClientException {
        // 默认推送模式消费者，createOrder消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ORDER_CREATE");
        // 设置NameSrv地址，用来路由寻找Broker
        consumer.setNamesrvAddr("localhost:9876");
        // 设置订阅Topic与Tag，tag为*代表所有的不过滤
        consumer.subscribe("ORDER", "createOrder");
        // 注册消息监听者，用来处理从broker拉取过来的消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach((msg) -> {
                String msgBody = new String(msg.getBody(), StandardCharsets.UTF_8);
                System.out.printf("%s 收到消息： %s %n", Thread.currentThread().getName() , msgBody);
            });

            // ack该消息已经消费成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

        });
        // 启动consumer实例，建立NameSrv与Broker连接
        consumer.start();

        // 最后关闭消费者
//        consumer.shutdown();
    }
}
