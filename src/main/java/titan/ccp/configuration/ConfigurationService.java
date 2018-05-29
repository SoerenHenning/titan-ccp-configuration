package titan.ccp.configuration;

import org.apache.commons.configuration2.Configuration;

import redis.clients.jedis.Jedis;
import spark.Spark;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.model.sensorregistry.SensorRegistry;

public class ConfigurationService {

	private final Configuration config = Configurations.create();
	private final Jedis jedis;
	private final KafkaPublisher kafkaPublisher;

	public ConfigurationService() {
		this.jedis = new Jedis(this.config.getString("redis.host"), this.config.getInt("redis.port"));
		this.kafkaPublisher = new KafkaPublisher(this.config.getString("kafka.boostrap.servers"),
				this.config.getString("kafka.topic"));
	}

	public void start() {
		Spark.port(this.config.getInt("webserver.port"));

		if (this.config.getBoolean("webserver.cors")) {
			Spark.options("/*", (request, response) -> {

				final String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
				if (accessControlRequestHeaders != null) {
					response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
				}

				final String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
				if (accessControlRequestMethod != null) {
					response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
				}

				return "OK";
			});

			Spark.before((request, response) -> {
				response.header("Access-Control-Allow-Origin", "*");
			});
		}

		Spark.get("/sensor-registry/", (request, response) -> {
			final String redisResponse = this.jedis.get("sensor_registry");
			if (redisResponse == null) {
				response.status(500);
				return "Internal Server Error";
			} else {
				return redisResponse;
			}
		});

		Spark.put("/sensor-registry/", (request, response) -> {
			// TODO validation
			final SensorRegistry sensorRegistry = SensorRegistry.fromJson(request.body());
			final String json = sensorRegistry.toJson();
			final String redisResponse = this.jedis.set("sensor_registry", json);
			if ("OK".equals(redisResponse)) {
				this.kafkaPublisher.publish(Event.SENSOR_REGISTRY_CHANGED, json);
				response.status(204);
				return "";
			} else {
				response.status(500);
				return "Internal Server Error";
			}
		});

		Spark.after((request, response) -> {
			response.type("application/json");
		});
	}

	public void stop() {
		this.jedis.close();
		this.kafkaPublisher.close();
	}

	public static void main(final String[] args) {
		new ConfigurationService().start();
	}

}
