package message.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class SimpleRabbitMqProducer {
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("129.204.89.67");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("gzz_msg");
        connectionFactory.setPassword("gzz_msg");
        connectionFactory.setVirtualHost("gzz_msg");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        //通道开启confirm机制，rabbitMQ会告诉生产者消息有没有投递成功
        channel.confirmSelect();
        channel.exchangeDeclare("gzz_msg_exchange", BuiltinExchangeType.DIRECT,true);
        //持久化队列
        channel.queueDeclare("gzz_sms_queue",true,false,false,null);
        channel.queueBind("gzz_sms_queue","gzz_msg_exchange","sms");
        while (true){
            System.out.print("你想给RabbitMq投递：");
            Scanner scanner = new Scanner(System.in);
            String next = scanner.next();
            //发送持久化消息
            channel.basicPublish("gzz_msg_exchange","sms",MessageProperties.PERSISTENT_TEXT_PLAIN,next.getBytes());
            //生产者确认机制
            boolean isOk = channel.waitForConfirms();
            if(isOk){
                System.out.println("投递成功！");
            }else {
                //重新投递
            }
            if(next.equals("exit")){
                break;
            }
        }
        channel.close();
        connection.close();
    }
}
