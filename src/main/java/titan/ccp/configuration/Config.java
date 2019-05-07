package titan.ccp.configuration;

import org.apache.commons.configuration2.Configuration;
import titan.ccp.common.configuration.Configurations;

public class Config {

  private final Configuration config = Configurations.create();

  public final String REDIS_HOST = this.config.getString(ConfigurationKeys.REDIS_HOST);
  public final int REDIS_PORT = this.config.getInt(ConfigurationKeys.REDIS_PORT);
  public final boolean DEMO = this.config.getBoolean(ConfigurationKeys.DEMO);
  public final boolean EVENT_PUBLISHING =
      this.config.getBoolean(ConfigurationKeys.EVENT_PUBLISHING);
  public final String KAFKA_TOPIC =
      this.config.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS);
  public final String KAFKA_BOOTSTRAP_SERVERS =
      this.config.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS);
  public final int FAILSAFE_DELAYINMILLIS =
      this.config.getInt(ConfigurationKeys.FAILSAFE_DELAYINMILLIS);
  public final int FAILSAFE_MAXRETRIES = this.config.getInt(ConfigurationKeys.FAILSAFE_MAXRETRIES);
  public final int WEBSERVER_PORT = this.config.getInt(ConfigurationKeys.WEBSERVER_PORT);
  public final boolean CORS = this.config.getBoolean(ConfigurationKeys.CORS));

  private static Config instance;

  /**
   * Creates a singleton instance of this class.
   *
   * @return Singleton instance of this class.
   */
  public static synchronized Config create() {

    if (instance == null) {
      instance = new Config();
    }

    return instance;
  }

}
