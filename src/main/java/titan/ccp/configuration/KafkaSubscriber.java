package titan.ccp.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaSubscriber {

	private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

	private static final String GROUP_ID = "test-2";

	private static final String TOPIC_NAME = "test-soeren--3";

	private final KafkaConsumer<Event, String> consumer;

	private final String topicName;

	private final Map<Event, List<Consumer<String>>> subscriptions = new EnumMap<>(Event.class);

	private volatile boolean terminationRequested = false;
	private final CompletableFuture<Void> terminationRequestResult = new CompletableFuture<>();

	public KafkaSubscriber() {
		final Properties properties = new Properties();

		properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		properties.put("group.id", GROUP_ID);
		// properties.put("enable.auto.commit", this.enableAutoCommit);
		// properties.put("auto.commit.interval.ms", this.autoCommitIntervalMs);
		// properties.put("session.timeout.ms", this.sessionTimeoutMs);

		this.consumer = new KafkaConsumer<>(properties, EventSerde.deserializer(), new StringDeserializer());
		this.topicName = TOPIC_NAME;

		this.run();
	}

	public void run() {
		this.consumer.subscribe(Arrays.asList(this.topicName));

		while (!this.terminationRequested) {
			final ConsumerRecords<Event, String> records = this.consumer.poll(1000); // TODO
			for (final ConsumerRecord<Event, String> record : records) {
				for (final Consumer<String> subscription : this.subscriptions.get(record.key())) {
					subscription.accept(record.value());
				}
			}
		}

		this.consumer.close();
		this.terminationRequestResult.complete(null);
	}

	public void subscribe(final Event event, final Consumer<String> action) {
		this.subscriptions.computeIfAbsent(event, x -> new ArrayList<>(4)).add(action);
	}

	public CompletableFuture<Void> requestTermination() {
		this.terminationRequested = true;
		return this.terminationRequestResult;
	}

}
