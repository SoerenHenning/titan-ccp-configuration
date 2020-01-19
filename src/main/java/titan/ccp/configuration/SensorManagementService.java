package titan.ccp.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.configuration.api.RestApiServer;
import titan.ccp.configuration.api.SensorHierarchyRepository;

/**
 * A microservice that manages the system-wide configuration. For example, the sensor registry. It
 * provides a REST interface to get or modify the configuration and optionally publishes changes to
 * a Kafka topic.
 *
 */
public final class SensorManagementService {

  private static final Logger LOGGER = LoggerFactory.getLogger(SensorManagementService.class);

  private RestApiServer webServer;

  private SensorHierarchyRepository sensorHierarchyRepository;

  /**
   * Run the microservice.
   */
  public void run() {
    this.sensorHierarchyRepository =
        new SensorHierarchyRepository(Config.MONGODB_HOST, Config.MONGODB_PORT);

    this.webServer = new RestApiServer(
        Config.WEBSERVER_PORT,
        Config.CORS,
        this.sensorHierarchyRepository);
    this.webServer.start();
  }

  /**
   * Stop the microservice.
   */
  public void stop() {
    LOGGER.warn("Shutting down Configuration microservice.");
    if (this.webServer != null) {
      this.webServer.stop();
    }
    if (this.sensorHierarchyRepository != null) {
      this.sensorHierarchyRepository.stop();
    }
    System.exit(1); // NOPMD exit application manually
  }

  public static void main(final String[] args) {
    new SensorManagementService().run();
  }
}
