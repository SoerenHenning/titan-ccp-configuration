package titan.ccp.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A microservice that manages the system-wide configuration. For example, the sensor registry. It
 * provides a REST interface to get or modify the configuration and optionally publishes changes to
 * a Kafka topic.
 *
 */
public final class ConfigurationService {

  private static final String REDIS_SENSOR_REGISTRY_KEY = "sensor_registry";

  private static final String SENSOR_REGISTRY_PATH = "/sensor-registry";

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationService.class);
  private final Config config = Config.create();
  private final ConfigurationRepository configurationRepository = new ConfigurationRepository();

  private RestApiServer webServer;


  public void run() {
    this.webServer = new RestApiServer(this.config.WEBSERVER_PORT, this.config.CORS);
    this.webServer.start();
  }



  public static void main(final String[] args) {
    new ConfigurationService().run();
  }

}
