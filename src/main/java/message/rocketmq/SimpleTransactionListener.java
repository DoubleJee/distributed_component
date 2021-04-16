package message.rocketmq;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;

// 事务监听器
public class SimpleTransactionListener implements TransactionListener {

    private ConcurrentHashMap<String, LocalTransactionState> localTrans = new ConcurrentHashMap<>();


    // 发送消息成功的时候，服务器会通知客户端，可以执行本地事务了
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        localTrans.put(msg.getTransactionId(), LocalTransactionState.UNKNOW);
        return LocalTransactionState.UNKNOW;
    }


    // 服务器定期检查事务状态，如果返回事务成功，就会将消息给消费者消费，回滚则不会给消费者消费
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        localTrans.put(msg.getTransactionId(), LocalTransactionState.ROLLBACK_MESSAGE);
        return localTrans.get(msg.getTransactionId());
    }
}
