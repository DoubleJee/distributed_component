package message.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SimpleRabbitMqCustomer {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("129.204.89.67");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("gzz_msg");
        connectionFactory.setPassword("gzz_msg");
        connectionFactory.setVirtualHost("gzz_msg");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicConsume("gzz_sms_queue",false,new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body);
                System.out.println("接受到消息：" + msg);
                System.out.println(consumerTag);
                //ack手动应答模式
                channel.basicAck(envelope.getDeliveryTag(),false);
            }
        });
    }
}
