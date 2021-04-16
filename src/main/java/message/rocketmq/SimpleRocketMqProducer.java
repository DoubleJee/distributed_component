package message.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class SimpleRocketMqProducer {

    public static void main(String[] args) {
//        Scanner scanner = new Scanner(System.in);
//        while (true){
//            System.out.print("请输入你的消息：");
//            String next = scanner.next();
//            if (next.equalsIgnoreCase("n")){
//                break;
//            }
//            asyncSend(next);
//        }

    }


    /**
     * 同步发送消息
     */
    static void syncSend(String msg) {
        // 默认MQ生产者
        DefaultMQProducer producer = null;
        try {
            // createOrder生产者组
            producer = new DefaultMQProducer("ORDER_CREATE");
            // 设置NameSrv地址，用来路由寻找Broker
            producer.setNamesrvAddr("localhost:9876");
            // 启动Producer实例，建立NameSrv与Broker连接
            producer.start();
            // 创建一个topic
            producer.createTopic("TBW102", "ORDER", 1);
            // 创建一个消息
            Message message = new Message("ORDER", "createOrder", msg.getBytes(StandardCharsets.UTF_8));
            // 同步发送消息
            SendResult send = producer.send(message);
            if (send.getSendStatus() != SendStatus.SEND_OK){
                System.err.println("发送失败");
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

    /**
     * 异步发送消息
     */
    static void asyncSend(String msg){
        // 默认MQ生产者
        DefaultMQProducer producer = null;
        try {
            // createOrder生产者组
            producer = new DefaultMQProducer("ORDER_CREATE");
            // 设置NameSrv地址，用来路由寻找Broker
            producer.setNamesrvAddr("localhost:9876");
            // 启动Producer实例，建立NameSrv与Broker连接
            producer.start();
            // 创建一个topic
            producer.createTopic("TBW102", "ORDER", 1);
            // 设置发送失败重试时间
            producer.setRetryTimesWhenSendAsyncFailed(0);
            // 创建一个消息
            Message message = new Message("ORDER", "createOrder", msg.getBytes(StandardCharsets.UTF_8));
            // 异步发送消息，且绑定回调实例，用于接收异步返回结果
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {

                }

                @Override
                public void onException(Throwable e) {
                    System.err.printf("发送失败，原因: %s", e);
                }
            });


        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // 异步发送不能立马关闭
//            if (producer != null){
//                producer.shutdown();
//            }
        }
    }
}
