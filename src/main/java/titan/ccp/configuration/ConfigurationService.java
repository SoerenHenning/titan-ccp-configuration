package titan.ccp.configuration;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import spark.Spark;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventPublisher;
import titan.ccp.configuration.events.KafkaPublisher;
import titan.ccp.configuration.events.NoopPublisher;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * A microservice that manages the system-wide configuration. For example, the sensor registry. It
 * provides a REST interface to get or modify the configuration and optionally publishes changes to
 * a Kafka topic.
 *
 */
public class ConfigurationService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationService.class);

  private static final String REDIS_SENSOR_REGISTRY_KEY = "sensor_registry";

  private static final String SENSOR_REGISTRY_PATH = "/sensor-registry";

  private static final String INTERNAL_SERVER_ERROR_MESSAGE = "Internal Server Error";
  private static final String ACCESS_FORBIDDEN_MESSAGE = "Access forbidden";

  private final Configuration config = Configurations.create();
  private Jedis jedis;
  private final EventPublisher eventPublisher;
  private final RetryPolicy<Object> retryPolicy = new RetryPolicy<>();

  /**
   * Create a new instance of the {@link ConfigurationService} using parameters configured
   * externally (environment variables or a .properties file).
   */
  public ConfigurationService() {
    final long startupWait = this.config.getLong("startup.wait.ms");
    LOGGER.info("Wait for {} ms to connect to redis.", startupWait);
    try {
      Thread.sleep(startupWait);
    } catch (final InterruptedException e) {
      throw new IllegalStateException(e);
    }

    // setup redis connection
    this.jedis = new Jedis(this.config.getString("redis.host"), this.config.getInt("redis.port"));

    // setup failsafe for redis
    final int delay = this.config.getInt("failsafe.delayInMillis");
    this.retryPolicy.handle(JedisConnectionException.class)
        .withDelay(Duration.ofMillis(delay))
        .withMaxRetries(this.config.getInt("failsafe.maxRetries"))
        .onFailedAttempt(i -> {
          this.jedis.close();
          this.jedis =
              new Jedis(this.config.getString("redis.host"), this.config.getInt("redis.port"));
          LOGGER.warn("Redis not available. Will retry in " + delay + "ms.");
        })
        .onSuccess(i -> LOGGER.info("Connected to redis"));

    // check if connection was successful.
    Failsafe.with(this.retryPolicy).run(() -> {
      this.jedis.ping();
    });

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
    if (this.config.getBoolean("demo")) { // NOCS
      LOGGER.info("Running in demo mode.");
    }
    this.setDefaultSensorRegistry();


    final String currentSensorRegistry = Failsafe.with(this.retryPolicy)
        .get(() -> this.jedis.get(REDIS_SENSOR_REGISTRY_KEY));

    if (currentSensorRegistry != null) {
      this.eventPublisher.publish(Event.SENSOR_REGISTRY_STATUS, currentSensorRegistry);
    }

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
      final String redisResponse;
      try {
        redisResponse = this.jedis.get(REDIS_SENSOR_REGISTRY_KEY);
      } catch (final JedisConnectionException e) {
        LOGGER.error("failed to connect to redis instance");
        response.status(500);
        return INTERNAL_SERVER_ERROR_MESSAGE;
      }

      if (redisResponse == null) {
        response.status(500); // NOCS HTTP response code
        return INTERNAL_SERVER_ERROR_MESSAGE;
      } else {
        return redisResponse;
      }
    });

    Spark.put(SENSOR_REGISTRY_PATH, (request, response) -> {
      if (this.config.getBoolean("demo")) { // NOCS
        response.status(403); // NOCS HTTP response code
        return ACCESS_FORBIDDEN_MESSAGE;
      } else {
        // TODO validation
        final SensorRegistry sensorRegistry = SensorRegistry.fromJson(request.body());
        final String json = sensorRegistry.toJson();
        final String redisResponse;
        try {
          redisResponse = this.jedis.set(REDIS_SENSOR_REGISTRY_KEY, json);
        } catch (final JedisConnectionException e) {
          LOGGER.error("failed to connect to redis instance");
          response.status(500);
          return INTERNAL_SERVER_ERROR_MESSAGE;
        }
        if ("OK".equals(redisResponse)) {
          this.eventPublisher.publish(Event.SENSOR_REGISTRY_CHANGED, json);
          response.status(204); // NOCS HTTP response code
          return "";
        } else {
          response.status(500); // NOCS HTTP response code
          return INTERNAL_SERVER_ERROR_MESSAGE;
        }
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

  private void setDefaultSensorRegistry() {
    final boolean isDemo = this.config.getBoolean("demo"); // NOCS
    final String sensorRegistry =
        isDemo ? this.getDemoSensorRegistry() : this.getEmptySensorRegsitry();
    Failsafe.with(this.retryPolicy)
        .run(() -> this.jedis.set(REDIS_SENSOR_REGISTRY_KEY, sensorRegistry));
    LOGGER.info("Set sensor registry");
  }

  private String getDemoSensorRegistry() {
    try {
      final URL url = Resources.getResource("demo_sensor_registry.json");
      return Resources.toString(url, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private String getEmptySensorRegsitry() {
    return new MutableSensorRegistry("root").toJson();
  }

  public static void main(final String[] args) {
    new ConfigurationService().start();
  }

}
