package titan.ccp.configuration;

/**
 * A microservice that manages the system-wide configuration. For example, the sensor registry. It
 * provides a REST interface to get or modify the configuration and optionally publishes changes to
 * a Kafka topic.
 *
 */
public final class ConfigurationService {

  private RestApiServer webServer; // NOPMD

  public void run() {
    this.webServer = new RestApiServer(Config.WEBSERVER_PORT, Config.CORS);
    this.webServer.start();
  }

  public static void main(final String[] args) {
    new ConfigurationService().run();
  }

}
