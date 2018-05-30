package titan.ccp.configuration;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaPublisher implements EventPublisher {

	private final String topic;

	private final Producer<Event, String> producer;

	public KafkaPublisher(final String bootstrapServers, final String topic) {
		this(bootstrapServers, topic, new Properties());
	}

	public KafkaPublisher(final String bootstrapServers, final String topic, final Properties defaultProperties) {
		this.topic = topic;

		final Properties properties = new Properties();
		properties.putAll(defaultProperties);
		properties.put("bootstrap.servers", bootstrapServers);
		// properties.put("acks", this.acknowledges);
		// properties.put("batch.size", this.batchSize);
		// properties.put("linger.ms", this.lingerMs);
		// properties.put("buffer.memory", this.bufferMemory);

		this.producer = new KafkaProducer<>(properties, EventSerde.serializer(), new StringSerializer());
	}

	@Override
	public void publish(final Event event, final String value) {
		final ProducerRecord<Event, String> record = new ProducerRecord<>(this.topic, event, value);
		this.producer.send(record);
	}

	@Override
	public void close() {
		this.producer.close();
	}

}
