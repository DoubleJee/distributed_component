package message.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

// 延迟消息，生产者发送，消费者不会立马收到
// 延迟消息会被存储到SCHEDULE_TOPIC_XXXX的Topic中，等到时间到了，再写入真实的Topic中
public class DelayMessage {


    public static void main(String[] args) throws MQClientException {
        producer("延迟的消息");
        customer();
    }


    static void producer(String msg) {
        DefaultMQProducer producer = null;
        try {
            // 实例化一个生产者来产生延时消息
            producer = new DefaultMQProducer("DELAY_TOPIC");
            producer.setNamesrvAddr("localhost:9876");
            // 启动生产者
            producer.start();
            producer.createTopic("TBW102", "DELAY_TOPIC", 3);
            Message message = new Message("DELAY_TOPIC", msg.getBytes(StandardCharsets.UTF_8));
            // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h，这是delayTimeLevel等级，下标从0开始
            // 设置延时等级4,这个消息将在10s之后发送 (现在只支持固定的几个时间,详看delayTimeLevel)
            message.setDelayTimeLevel(4);
            // 同步发送
            producer.send(message);

        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (producer != null) {
                producer.shutdown();
            }
        }
    }

    static void customer() throws MQClientException {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("DELAY_TOPIC");
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅Topics
        consumer.subscribe("DELAY_TOPIC", "*");
        // 注册消息监听者
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach((msg) -> {
                String msgBody = new String(msg.getBody(), StandardCharsets.UTF_8);
                System.out.printf("收到消息：[msgId=%s] %ds later，消息体：[%s]", msg.getMsgId() , (System.currentTimeMillis() - msg.getBornTimestamp()) / 1000, msgBody);
            });

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

        });
        consumer.start();
    }




}
