//Copyright (c) Microsoft Corporation. All rights reserved.
//Licensed under the MIT License.
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;

public class TestDataReporter implements Runnable {

    private static final int NUM_MESSAGES = 10000;
    private final String TOPIC;

    private Producer<Long, String> producer;

    public TestDataReporter(final Producer<Long, String> producer, String TOPIC) {
        this.producer = producer;
        this.TOPIC = TOPIC;
    }

    @Override
    public void run() {
        for(int i = 0; i < NUM_MESSAGES; i++) {                
            long time = System.currentTimeMillis();
            System.out.println("Test Data #" + i + " from thread #" + Thread.currentThread().getId());

            // Read in SampleData.json file
           String sampleData = "SAMPLE DATA";

           try {
               sampleData = new String(Files.readAllBytes(Paths.get("./SampleData.json")));
           } catch(IOException ex) {
               System.out.println("Invalid JSON File");
           }
            
            final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, time, sampleData);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println(exception);
                        System.exit(1);
                    }
                }
            });

            // Pause for 10 seconds after sending data
            try {
                Thread.sleep(10000); 
            } catch(InterruptedException ex){
                Thread.currentThread().interrupt(); 
            }
        }
        System.out.println("Finished sending " + NUM_MESSAGES + " messages from thread #" + Thread.currentThread().getId() + "!");
    }
}