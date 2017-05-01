import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordResult;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

/**
 * Created by aganapathy on 4/30/17.
 */
public class TransactionLoggingKinesisProducer extends AbstractKinesisProducer {

    private final AmazonKinesis kinesis;



    public TransactionLoggingKinesisProducer(BlockingQueue<TransactionLogging> inputQueue) {
        super(inputQueue);
        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception ex) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct ", ex);
        }
        kinesis = new AmazonKinesisClient(credentialsProvider);


    }



    @Override protected void runOnce() throws Exception {

        TransactionLogging transactionLogging = inputQueue.take();
        String partitionKey = transactionLogging.getSessionId();
        ByteBuffer data = ByteBuffer.wrap(
                transactionLogging.getPayload().getBytes("UTF-8"));
        PutRecordResult putRecordResult = kinesis.putRecord(STREAM_NAME, data, partitionKey);
        recordsPut.getAndIncrement();
        System.out.println("Sequence no : "+putRecordResult.getSequenceNumber());
        System.out.println("Shard id : "+putRecordResult.getShardId());
        System.out.println("recordsPut : "+recordsPut.toString());


    }


}
