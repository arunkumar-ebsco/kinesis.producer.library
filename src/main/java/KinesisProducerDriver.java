import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by aganapathy on 4/30/17.
 */
public class KinesisProducerDriver {

    private static TransactionLogging generateTransactionLogging(){
        return  new TransactionLogging("001","Sample Payload");
    }

    public static void main(String[] args) throws Exception {
        final BlockingQueue<TransactionLogging> txnLoggingQueue = new ArrayBlockingQueue<TransactionLogging>(10);
        final ExecutorService exec = Executors.newCachedThreadPool();
        final AbstractKinesisProducer worker = new TransactionLoggingKinesisProducer(txnLoggingQueue);
        exec.execute(worker);

        exec.execute(() ->{
            try {
                txnLoggingQueue.put(generateTransactionLogging());
                worker.stop();
                exec.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
