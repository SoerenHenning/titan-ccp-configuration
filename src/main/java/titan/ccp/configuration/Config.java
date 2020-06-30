package titan.ccp.configuration;

import org.apache.commons.configuration2.Configuration;
import titan.ccp.common.configuration.ServiceConfigurations;

/**
 * Utility class that wraps the access to the config files.
 */
public final class Config { // NOPMD utility class alert

  private static final Configuration CONFIGURATION = ServiceConfigurations.createWithDefaults();

  public static final String REDIS_HOST =
      CONFIGURATION.getString(ConfigurationKeys.REDIS_HOST);
  public static final int REDIS_PORT = Config.CONFIGURATION.getInt(ConfigurationKeys.REDIS_PORT);
  public static final boolean DEMO = Config.CONFIGURATION.getBoolean(ConfigurationKeys.DEMO);
  public static final boolean EVENT_PUBLISHING =
      CONFIGURATION.getBoolean(ConfigurationKeys.EVENT_PUBLISHING);
  public static final String KAFKA_TOPIC =
      CONFIGURATION.getString(ConfigurationKeys.KAFKA_TOPIC);
  public static final String KAFKA_BOOTSTRAP_SERVERS =
      CONFIGURATION.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS);
  public static final int FAILSAFE_DELAYINMILLIS =
      CONFIGURATION.getInt(ConfigurationKeys.FAILSAFE_DELAYINMILLIS);
  public static final int FAILSAFE_MAXRETRIES =
      CONFIGURATION.getInt(ConfigurationKeys.FAILSAFE_MAXRETRIES);
  public static final int WEBSERVER_PORT =
      CONFIGURATION.getInt(ConfigurationKeys.WEBSERVER_PORT);
  public static final boolean CORS = CONFIGURATION.getBoolean(ConfigurationKeys.CORS);
  public static final String INITIAL_SENSOR_REGISTRY =
      CONFIGURATION.getString("initial.sensor.registry");

  private Config() {}

}
