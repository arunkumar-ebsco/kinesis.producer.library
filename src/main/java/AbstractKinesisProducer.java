import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by aganapathy on 4/30/17.
 */
public abstract class AbstractKinesisProducer implements Runnable{

    protected final static String STREAM_NAME = "arun_test_stream";
    protected final static String REGION = "us-east-1";
    protected final BlockingQueue<TransactionLogging> inputQueue;
    protected volatile boolean shutdown = false;
    protected final AtomicLong recordsPut = new AtomicLong(0);

    public AbstractKinesisProducer(BlockingQueue<TransactionLogging> inputQueue) {
        this.inputQueue = inputQueue;
    }



    @Override
    public void run() {
        while (!shutdown) {
            try {
                runOnce();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public long recordsPut() {
        return recordsPut.get();
    }

    public void stop() {
        shutdown = true;
    }

    protected abstract void runOnce() throws Exception;
}
