package titan.ccp.configuration;

import org.apache.commons.configuration2.Configuration;
import titan.ccp.common.configuration.Configurations;

/**
 * Utility class that wraps the access to the config files.
 */
public final class Config { // NOPMD

  private static final Configuration CONFIGURATION = Configurations.create();

  public static final String REDIS_HOST =
      Config.CONFIGURATION.getString(ConfigurationKeys.REDIS_HOST);
  public static final int REDIS_PORT = Config.CONFIGURATION.getInt(ConfigurationKeys.REDIS_PORT);
  public static final boolean DEMO = Config.CONFIGURATION.getBoolean(ConfigurationKeys.DEMO);
  public static final boolean EVENT_PUBLISHING =
      Config.CONFIGURATION.getBoolean(ConfigurationKeys.EVENT_PUBLISHING);
  public static final String KAFKA_TOPIC =
      Config.CONFIGURATION.getString(ConfigurationKeys.KAFKA_TOPIC);
  public static final String KAFKA_BOOTSTRAP_SERVERS =
      Config.CONFIGURATION.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS);
  public static final int FAILSAFE_DELAYINMILLIS =
      Config.CONFIGURATION.getInt(ConfigurationKeys.FAILSAFE_DELAYINMILLIS);
  public static final int FAILSAFE_MAXRETRIES =
      Config.CONFIGURATION.getInt(ConfigurationKeys.FAILSAFE_MAXRETRIES);
  public static final int WEBSERVER_PORT =
      Config.CONFIGURATION.getInt(ConfigurationKeys.WEBSERVER_PORT);
  public static final boolean CORS = Config.CONFIGURATION.getBoolean(ConfigurationKeys.CORS);

  private Config() {}

}
