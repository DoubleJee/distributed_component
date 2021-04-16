package message.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * 过滤消息，因为RocketMQ限制，消息只有一个Tag，在复杂场景可能起不到作用
 * RocketMQ支持SQL92标准的sql语句来过滤消息，符合条件的查询到
 * 配置Broker的enablePropertyFilter属性为true，才可以支持过滤
 *
 * 生产者设置消息属性
 * 消费者通过消息属性，过滤消息
 * 由客户端过滤消息，服务器会推送过来
 *
 */
public class FilterMessage {


    public static void main(String[] args) throws MQClientException {
        send("love");
        receive();
    }


    static void send(String msg)  {
        DefaultMQProducer producer = null;
        try {
            producer = new DefaultMQProducer("FILTER_MESSAGE");
            producer.setNamesrvAddr("localhost:9876");
            producer.start();
            producer.createTopic("TBW102", "FILTER_MESSAGE_TOPIC", 1);
            for (int i = 0; i < 20; i++) {
                Message message = new Message("FILTER_MESSAGE_TOPIC", "", String.valueOf(i), msg.getBytes(StandardCharsets.UTF_8));

                // 设置用户属性
                message.putUserProperty("name", "李四");
                message.putUserProperty("age", String.valueOf(i));
                producer.send(message);
            }


        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (producer != null){
                producer.shutdown();
            }
        }
    }

    static void receive() throws MQClientException {
        // 完整业务线消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("FILTER_MESSAGE");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 订阅Topic，并且消息过滤
        // 只有订阅的消息有这个属性age, age >= 10
        consumer.subscribe("FILTER_MESSAGE_TOPIC", MessageSelector.bySql("age >= 10"));
        consumer.registerMessageListener(((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                String msgBody = new String(msg.getBody(), StandardCharsets.UTF_8);
                String age = msg.getUserProperty("age");
                System.out.printf("%s queueId=[%d]，收到消息： %s，用户属性age:[%d] %n", Thread.currentThread().getName(), msg.getQueueId(), msgBody, Integer.valueOf(age));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }));
        consumer.start();
    }



}
