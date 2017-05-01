import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

/**
 * Created by aganapathy on 4/30/17.
 */
public class TransactionLoggingKinesisProducer extends AbstractKinesisProducer {

    private final AmazonKinesis kinesis;



    public TransactionLoggingKinesisProducer(BlockingQueue<TransactionLogging> inputQueue) {
        super(inputQueue);
        AWSCredentials credentials = new BasicAWSCredentials("XXX", "YYY");
        kinesis = new AmazonKinesisClient(credentials);

    }



    @Override protected void runOnce() throws Exception {

        TransactionLogging transactionLogging = inputQueue.take();
        String partitionKey = transactionLogging.getSessionId();
        ByteBuffer data = ByteBuffer.wrap(
                transactionLogging.getPayload().getBytes("UTF-8"));
        kinesis.putRecord(STREAM_NAME, data, partitionKey);
        recordsPut.getAndIncrement();
        System.out.println("recordsPut : "+recordsPut.toString());


    }
}
