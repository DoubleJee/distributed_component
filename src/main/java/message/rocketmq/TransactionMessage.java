package message.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// 事务消息，
// 1.MQ接收到事务消息后，会通知对应的事务生产者可以执行本地事务了
// 2.MQ会定期检查事务状态，如果返回事务成功，就会将消息给消费者消费，回滚则不会给消费者消费
public class TransactionMessage {

    public static void main(String[] args) throws MQClientException {
        producer("love");
        customer();
    }


    static void producer(String msg) throws MQClientException {
        // 1.实例化事务消息生产者   TRANSACTION_TOPIC生产者组
        TransactionMQProducer producer = new TransactionMQProducer("TRANSACTION_TOPIC");
        // 2.设置NameSrv地址，用来路由寻找Broker
        producer.setNamesrvAddr("localhost:9876");
        // 3.自定义线程池处理MQ检查请求
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), (r) -> {
            Thread thread = new Thread(r);
            thread.setName("client-transaction-msg-check-thread");
            return thread;
        });
        producer.setExecutorService(executorService);

        // 4.设置事务监听器，用来处理MQ的检查请求，和执行事务
        producer.setTransactionListener(new SimpleTransactionListener());
        // 5.启动Producer实例，建立NameSrv与Broker连接
        producer.start();
        // 6.创建一个topic
        producer.createTopic("TBW102", "TRANSACTION_TOPIC", 1);
        // 7.构建消息
        Message message = new Message("TRANSACTION_TOPIC", msg.getBytes(StandardCharsets.UTF_8));
        TransactionSendResult transactionSendResult = producer.sendMessageInTransaction(message, null);
        LocalTransactionState localTransactionState = transactionSendResult.getLocalTransactionState();
        System.out.println(localTransactionState);
    }

    static void customer() throws MQClientException {
        // 默认推送模式消费者，createOrder消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TRANSACTION_TOPIC");
        // 设置NameSrv地址，用来路由寻找Broker
        consumer.setNamesrvAddr("localhost:9876");
        // 设置订阅Topic与Tag，tag为*代表所有的不过滤
        consumer.subscribe("TRANSACTION_TOPIC", "*");
        // 注册消息监听者，用来处理从broker拉取过来的消息，并发处理
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
    }
}
