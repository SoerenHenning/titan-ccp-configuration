package titan.ccp.configuration;

import java.time.Duration;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Wrapper for the database access for the sensor registry.
 */
public final class ConfigurationRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationRepository.class);

  private static final String REDIS_SENSOR_REGISTRY_KEY = "sensor_registry";

  private static final String REDIS_CONNECTION_ERROR_MESSAGE =
      "Failed to connect to redis instance.";

  private final Jedis jedis;

  private RetryPolicy<Object> jedisRetryPolicy;

  /**
   * Create the repository.
   */
  public ConfigurationRepository() {

    // setup failsafe for redis
    this.setupFailsafe();

    // get jedis instance
    this.jedis = this.getJedisInstance();

    // check if connection was successful.
    Failsafe.with(this.jedisRetryPolicy).run(() -> {
      this.jedis.ping();
    });

  }

  /**
   * Sets up a connection to redis that is encapsulated by the failsafe-framework.
   */
  public void setupFailsafe() {
    this.jedisRetryPolicy = new RetryPolicy<>();
    this.jedisRetryPolicy.handle(JedisConnectionException.class)
        .withDelay(Duration.ofMillis(Config.FAILSAFE_DELAYINMILLIS))
        .withMaxRetries(Config.FAILSAFE_MAXRETRIES)
        .onFailedAttempt(i -> {
          this.jedis.close();
          LOGGER.warn("Redis not available. Will retry in {} ms.",
              Config.FAILSAFE_DELAYINMILLIS);
        })
        .onSuccess(i -> LOGGER.info("Connected to redis"));
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
      LOGGER.error(REDIS_CONNECTION_ERROR_MESSAGE);
      throw new ConfigurationRepositoryException(); // NOPMD
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
        return;
      } else {
        throw new ConfigurationRepositoryException();
      }
    } catch (final JedisConnectionException e) {
      LOGGER.error(REDIS_CONNECTION_ERROR_MESSAGE);
      throw new ConfigurationRepositoryException(); // NOPMD
    }
  }

  /**
   * Set default configuration to the database. The operation is encapsulated by the failsafe
   * framework.
   *
   * @param sensorRegistry The sensor-registry that should be persisted.
   */
  public void putConfigurationSafe(final String sensorRegistry) {
    Failsafe.with(this.jedisRetryPolicy)
        .run(() -> this.jedis.set(REDIS_SENSOR_REGISTRY_KEY, sensorRegistry));
  }



  /**
   * Get a new {@link Jedis} instance. This method should be called when the old Jedis instance is
   * broken, e.g. due to an connection failure.
   *
   * @return A new {@link Jedis} instance.
   */
  public Jedis getJedisInstance() {
    return new Jedis(Config.REDIS_HOST,
        Config.REDIS_PORT);
  }

  /**
   * Special exception for errors that might occur in this repository.
   *
   */
  @SuppressWarnings("serial")
  public class ConfigurationRepositoryException extends Exception {

  }
}
