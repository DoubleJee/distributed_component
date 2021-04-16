package message.rocketmq;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

// 全局顺序消息，topic只有一个queue，队列天然保证有序
// 分区顺序消息，消息通过sharding key分区到某个queue，队列天然保证有序
// 一个客户端订阅topic，接收消息时，客户端会独占topic的这个queue，客户端能独占多个queue（范围是同一个消费组）
// 由客户端使用的监听类来决定是顺序消费，还是并发消费

// 使用这个MessageListenerOrderly有序监听类，是实现顺序消费的关键
    public class OrderlyMessage {

    public static void main(String[] args) throws MQClientException {
        send();
        receive();

    }


    static void send() throws MQClientException {
        // 完整业务线生产者组
        DefaultMQProducer producer = new DefaultMQProducer("FULL_BUSINESS_LINE_CREATE_ORDER");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        // topic分片到5个队列存储消息，分区顺序
        producer.createTopic("TBW102", "FULL_BL_ORDER", 5);
        String[] tags = new String[]{"TagA", "TagC", "TagD"};
        List<OrderStep> orderSteps = orderSteps();

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sdf.format(date);

        for (int i = 0; i < orderSteps.size(); i++) {
            OrderStep orderStep = orderSteps.get(i);
            // 加个时间前缀
            String body = dateStr + ":" + orderStep;
            Message msg = new Message("FULL_BL_ORDER", tags[i % tags.length], "KEY" + i, body.getBytes());
            try {
                producer.send(msg, (mqs, msg1, arg) -> {
                    Long orderId = (Long) arg;
                    // 将orderId作为sharding key，进行分区队列，同一个orderId的消息会被放入同一个分区队列，此队列天然保证有序
                    long index = orderId % mqs.size();
                    // 选择分区队列
                    return mqs.get((int)index);

                }, orderStep.getOrderId());
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }

    static void receive() throws MQClientException {
        // 完整业务线消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("FULL_BUSINESS_LINE_CREATE_ORDER");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费，如果非第一次启动，那么按照上次消费的位置继续消费
        // 从头部消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅完整订单业务线，接收这三个的Tags消息
        consumer.subscribe("FULL_BL_ORDER", "TagA || TagC || TagD");


        // MessageListenerOrderly类，每个队列有唯一的customer线程消费（在消费者集群情况下也会保证），因此有序消费，是实现顺序消费的关键
        // MessageListenerConcurrently类，随机分配线程，每个队列有多个线程消费，因此并发消费
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            context.setAutoCommit(true);
            msgs.forEach((msg) -> {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String msgBody = new String(msg.getBody(), StandardCharsets.UTF_8);
                System.out.printf("[%s][%s] queueId：[%s]  [%s]%n", sdf.format(new Date()), Thread.currentThread().getName(), msg.getQueueId() , msgBody);
            });

            // 消费成功
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();

    }

    static List<OrderStep> orderSteps(){
        // 按照顺序放入
        List<OrderStep> orderList = new ArrayList<>();
        orderList.add(new OrderStep(15103111039L,"创建"));
        orderList.add(new OrderStep(15103111065L,"创建"));
        orderList.add(new OrderStep(15103111039L,"付款"));
        orderList.add(new OrderStep(15103117235L,"创建"));
        orderList.add(new OrderStep(15103111065L,"付款"));
        orderList.add(new OrderStep(15103117235L,"付款"));
        orderList.add(new OrderStep(15103111039L,"推送"));
        orderList.add(new OrderStep(15103117235L,"完成"));
        orderList.add(new OrderStep(15103111039L,"完成"));
//        orderList.add(new OrderStep(15103111041L,"创建"));
//        orderList.add(new OrderStep(15103111026L,"创建"));
//        orderList.add(new OrderStep(15103111098L,"创建"));
//        orderList.add(new OrderStep(15103111041L,"付款"));
//        orderList.add(new OrderStep(15103111026L,"付款"));
//        orderList.add(new OrderStep(15103111098L,"付款"));
//        orderList.add(new OrderStep(15103111041L,"推送"));
//        orderList.add(new OrderStep(15103111026L,"推送"));
//        orderList.add(new OrderStep(15103111098L,"推送"));
//        orderList.add(new OrderStep(15103111041L,"完成"));
//        orderList.add(new OrderStep(15103111026L,"完成"));
//        orderList.add(new OrderStep(15103111098L,"完成"));
        return orderList;
    }

    static class OrderStep {
        Long orderId;
        String StepName;

        public OrderStep(Long orderId, String stepName) {
            this.orderId = orderId;
            StepName = stepName;
        }
        public Long getOrderId() {
            return orderId;
        }

        public String getStepName() {
            return StepName;
        }

        @Override
        public String toString() {
            return "OrderStep{" +
                    "orderId=" + orderId +
                    ", StepName='" + StepName + '\'' +
                    '}';
        }
    }
}
