package titan.ccp.configuration.api;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import titan.ccp.configuration.Config;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventPublisher;
import titan.ccp.configuration.events.KafkaPublisher;
import titan.ccp.configuration.events.NoopPublisher;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;

/**
 * Wrapper for the database access for the sensor registry.
 */
public final class ConfigurationRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationRepository.class);

  private static final String REDIS_SENSOR_REGISTRY_KEY = "sensor_registry";

  private static final String REDIS_CONNECTION_ERR =
      "Failed to connect to redis instance.";

  private static final String REDIS_CONNECTION_RETRY_ERR =
      "Redis not available. Will retry in {} ms.";

  private static final String REDIS_CONNECTION_ABORT_ERR = "Connection to redis failed";

  private static final String REDIS_CONNECTION_WRITE_ERR =
      "Failed to write to redis.";

  private static final String REDIS_CONNECTION_SUCCESS = "Connected to redis";

  private Jedis jedis;

  private final EventPublisher eventPublisher;

  private RetryPolicy<Object> jedisRetryPolicy;

  /**
   * Create the repository.
   *
   * @throws ConfigurationRepositoryException When there occurs an error within the repository.
   */
  public ConfigurationRepository() throws ConfigurationRepositoryException {

    // setup failsafe for redis
    this.setupFailsafe();

    // get jedis instance
    this.jedis = this.getJedisInstance();

    // check if connection was successful.
    this.pingRedis();

    // setup event publishing
    if (Config.EVENT_PUBLISHING) {
      this.eventPublisher =
          new KafkaPublisher(Config.KAFKA_BOOTSTRAP_SERVERS, Config.KAFKA_TOPIC);
    } else {
      this.eventPublisher = new NoopPublisher();
    }

    this.setDefaultSensorRegistry();

  }

  /**
   * Sets up a connection to redis that is encapsulated by the failsafe-framework.
   */
  private void setupFailsafe() {
    this.jedisRetryPolicy = new RetryPolicy<>()
        .handle(JedisConnectionException.class)
        .withDelay(Duration.ofMillis(Config.FAILSAFE_DELAYINMILLIS))
        .withMaxRetries(Config.FAILSAFE_MAXRETRIES)
        .onFailedAttempt(i -> {
          this.jedis.close();
          this.jedis = this.getJedisInstance();
          LOGGER.warn(REDIS_CONNECTION_RETRY_ERR,
              Config.FAILSAFE_DELAYINMILLIS);
        })
        .onSuccess(i -> LOGGER.info(REDIS_CONNECTION_SUCCESS))
        .onFailure(i -> {
          LOGGER.error(REDIS_CONNECTION_ABORT_ERR);
        });
  }

  /**
   * Get the configuration from the database.
   *
   * @return The configuration as a JSON string.
   * @throws ConfigurationRepositoryException when the jedis connection fails.
   */
  public String getConfiguration() throws ConfigurationRepositoryException {
    try {
      return this.jedis.get(REDIS_SENSOR_REGISTRY_KEY);
    } catch (final JedisConnectionException e) {
      LOGGER.error(REDIS_CONNECTION_ERR, e);
      throw new ConfigurationRepositoryException(); // NOPMD
    }
  }

  /**
   * Ping redis database.
   *
   * @throws ConfigurationRepositoryException When redis is not availabe.
   */
  private void pingRedis() throws ConfigurationRepositoryException {
    try {
      Failsafe.with(this.jedisRetryPolicy).run(() -> {
        this.jedis.ping();
      });
    } catch (final JedisConnectionException e) {
      LOGGER.error(REDIS_CONNECTION_ERR, e);
      throw new ConfigurationRepositoryException(); // NOPMD rethrow exception
    }
  }

  /**
   * Save a configuration to the database.
   *
   * @param sensorRegistry The sensor-registry that should be persisted.
   * @throws ConfigurationRepositoryException When an error occurs in the repository.
   */
  public void putConfiguration(final String sensorRegistry)
      throws ConfigurationRepositoryException {
    try {
      final String response = this.jedis.set(REDIS_SENSOR_REGISTRY_KEY, sensorRegistry);
      if ("OK".equals(response)) {
        this.eventPublisher.publish(Event.SENSOR_REGISTRY_CHANGED, sensorRegistry);
        return;
      } else {
        LOGGER.error(REDIS_CONNECTION_WRITE_ERR);
        throw new ConfigurationRepositoryException();
      }
    } catch (final JedisConnectionException e) {
      LOGGER.error(REDIS_CONNECTION_ERR, e);
      throw new ConfigurationRepositoryException(); // NOPMD rethrow exception
    }
  }


  /**
   * Set initial configuration to the database. The operation is encapsulated and does not throw an
   * exception.
   *
   * @throws ConfigurationRepositoryException When the initial sensor registry cannot be put to the
   *         repository.
   */
  private void setDefaultSensorRegistry() throws ConfigurationRepositoryException {
    LOGGER.info("Set default sensor registry");

    final String sensorRegistry = this.getDefaultSensorRegsitry(); // NOPMD

    try {
      Failsafe.with(this.jedisRetryPolicy)
          .run(() -> this.jedis.set(REDIS_SENSOR_REGISTRY_KEY, sensorRegistry));
    } catch (final JedisConnectionException e) {
      LOGGER.error(REDIS_CONNECTION_ERR, e);
      throw new ConfigurationRepositoryException(); // NOPMD exception logged
    }

    this.eventPublisher.publish(Event.SENSOR_REGISTRY_STATUS, sensorRegistry);

  }

  /**
   * Get the default sensor-registry.
   *
   * @return The default sensor registry as JSON
   */
  private String getDefaultSensorRegsitry() {
    if (Config.DEMO) { // NOPMD if statement
      return this.getDemoSensorRegistry();
    } else {
      final String initial = this.getInitialSensorRegistry();
      if (initial != null) { // NOPMD if statement
        return initial;
      } else {
        return this.getEmptySensorRegistry();
      }
    }
  }

  /**
   * Get the inital sensor-registry.
   *
   * @return The inital sensor registry as JSON
   */
  private String getInitialSensorRegistry() {
    return Config.INITIAL_SENSOR_REGISTRY;
  }


  /**
   * Get the demo sensor-registry.
   *
   * @return The Demo Sensor-Registry
   */
  private String getDemoSensorRegistry() {
    try {
      final URL url = Resources.getResource("demo_sensor_registry.json");
      return Resources.toString(url, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      LOGGER.error("Demo sensor registry could not be loaded", e);
      throw new IllegalStateException(e);
    }
  }

  /**
   * Get the empty sensor-registry.
   *
   * @return
   */
  private String getEmptySensorRegistry() {
    return new MutableSensorRegistry("root").toJson();
  }


  /**
   * Get a new {@link Jedis} instance. This method should be called when the old Jedis instance is
   * broken, e.g. due to an connection failure.
   *
   * @return A new {@link Jedis} instance.
   */
  private Jedis getJedisInstance() {
    return new Jedis(Config.REDIS_HOST,
        Config.REDIS_PORT);
  }

  /**
   * Close the repository.
   */
  public void close() {
    this.jedis.close();
  }

  /**
   * Special exception for errors that might occur in this repository.
   *
   */
  @SuppressWarnings("serial")
  public class ConfigurationRepositoryException extends Exception {

  }
}
