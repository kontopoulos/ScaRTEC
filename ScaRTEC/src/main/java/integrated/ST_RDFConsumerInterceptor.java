package integrated;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Iterator;
import java.util.Map;

public class ST_RDFConsumerInterceptor implements ConsumerInterceptor {

    @Override
    public ConsumerRecords<String,ST_RDF> onConsume(ConsumerRecords records) {
        Iterator<ConsumerRecord<String,ST_RDF>> iterator = records.iterator();
        while (iterator.hasNext()) {
            ConsumerRecord<String,ST_RDF> currValue = iterator.next();
            currValue.value().setIngestionTimestamp(System.currentTimeMillis());
        }
        return records;
    }

    @Override
    public void close() {

    }

    @Override
    public void onCommit(Map map) {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
