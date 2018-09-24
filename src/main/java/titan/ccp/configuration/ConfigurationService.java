package titan.ccp.configuration;

import org.apache.commons.configuration2.Configuration;
import redis.clients.jedis.Jedis;
import spark.Spark;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventPublisher;
import titan.ccp.configuration.events.KafkaPublisher;
import titan.ccp.configuration.events.NoopPublisher;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * A microservice that manages the system-wide configuration. For example, the sensor registry. It
 * provides a REST interface to get or modify the configuration and optionally publishes changes to
 * a Kafka topic.
 *
 */
public class ConfigurationService {

  private static final String REDIS_SENSOR_REGISTRY_KEY = "sensor_registry";

  private static final String SENSOR_REGISTRY_PATH = "/sensor-registry";

  private static final String INTERNAL_SERVER_ERROR_MESSAGE = "Internal Server Error";

  private final Configuration config = Configurations.create();
  private final Jedis jedis;
  private final EventPublisher eventPublisher;

  /**
   * Create a new instance of the {@link ConfigurationService} using parameters configured
   * externally (environment variables or a .properties file).
   */
  public ConfigurationService() {
    this.jedis = new Jedis(this.config.getString("redis.host"), this.config.getInt("redis.port"));
    if (this.config.getBoolean("event.publishing")) {
      this.eventPublisher = new KafkaPublisher(this.config.getString("kafka.bootstrap.servers"),
          this.config.getString("kafka.topic"));
    } else {
      this.eventPublisher = new NoopPublisher();
    }
  }

  /**
   * Start the service by starting the underlying web server.
   */
  public void start() {
    Spark.port(this.config.getInt("webserver.port"));

    if (this.config.getBoolean("webserver.cors")) {
      Spark.options("/*", (request, response) -> {

        final String accessControlRequestHeaders =
            request.headers("Access-Control-Request-Headers");
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

    Spark.get(SENSOR_REGISTRY_PATH, (request, response) -> {
      final String redisResponse = this.jedis.get(REDIS_SENSOR_REGISTRY_KEY);
      if (redisResponse == null) {
        response.status(500); // NOCS HTTP response code
        return INTERNAL_SERVER_ERROR_MESSAGE;
      } else {
        return redisResponse;
      }
    });

    Spark.put(SENSOR_REGISTRY_PATH, (request, response) -> {
      // TODO validation
      final SensorRegistry sensorRegistry = SensorRegistry.fromJson(request.body());
      final String json = sensorRegistry.toJson();
      final String redisResponse = this.jedis.set(REDIS_SENSOR_REGISTRY_KEY, json);
      if ("OK".equals(redisResponse)) {
        this.eventPublisher.publish(Event.SENSOR_REGISTRY_CHANGED, json);
        response.status(204); // NOCS HTTP response code
        return "";
      } else {
        response.status(500); // NOCS HTTP response code
        return INTERNAL_SERVER_ERROR_MESSAGE;
      }
    });

    Spark.after((request, response) -> {
      response.type("application/json");
    });
  }

  public void stop() {
    this.jedis.close();
    this.eventPublisher.close();
  }

  public static void main(final String[] args) {
    new ConfigurationService().start();
  }

}
