package titan.ccp.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Service;
import titan.ccp.configuration.ConfigurationRepository.ConfigurationRepositoryException;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Webserver for the Configuration microservice.
 */
public final class RestApiServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

  private static final String ACCESS_FORBIDDEN_MESSAGE = "Access forbidden";

  private static final String INTERNAL_SERVER_ERROR_MESSAGE = "Internal Server Error";

  private static final String SENSOR_REGISTRY_PATH = "/sensor-registry";

  private final ConfigurationRepository configurationRepository;

  private final Service webService;

  private final boolean enableCors;

  /**
   * Creates a new webserver.
   */
  public RestApiServer(final int port, final boolean enableCors,
      final ConfigurationRepository configurationRepository) {
    LOGGER.info("Instantiating API server");

    this.configurationRepository = configurationRepository;

    this.webService = Service.ignite().port(port);
    this.enableCors = enableCors;
  }

  /**
   * Starts the service api.
   */
  public void start() {
    LOGGER.info("Starting API server");

    if (this.enableCors) {
      this.enableCorsHeaders();
    }

    // handle getting sensor registry
    this.webService.get(SENSOR_REGISTRY_PATH, (request, response) -> {
      return this.configurationRepository.getConfiguration();
    });

    // handle putting sensor registry
    this.webService.put(SENSOR_REGISTRY_PATH, (request, response) -> {
      if (Config.DEMO) {
        response.status(403); // NOCS HTTP response code
        return ACCESS_FORBIDDEN_MESSAGE;
      }

      // TODO validation
      final SensorRegistry sensorRegistry = SensorRegistry.fromJson(request.body());
      final String json = sensorRegistry.toJson();

      this.configurationRepository.putConfiguration(json);

      response.status(204); // NOCS HTTP response code
      return "";
    });

    // handle repository exceptions
    this.webService.exception(ConfigurationRepositoryException.class, (e, request, response) -> {
      response.status(500); // NOCS HTTP response code
      response.body(INTERNAL_SERVER_ERROR_MESSAGE);
    });
  }

  /**
   * Enable cors and set headers.
   */
  private void enableCorsHeaders() {

    this.webService.options("/*", (request, response) -> {

      final String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
      if (accessControlRequestHeaders != null) {
        response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
      }

      final String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
      if (accessControlRequestMethod != null) {
        response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
      }
      return "OK";
    });

    this.webService.before((request, response) -> {
      response.header("Access-Control-Allow-Origin", "*");
    });

    this.webService.after((request, response) -> {
      response.type("application/json");
    });
  }

  /**
   * Stop the webserver.
   */
  public void stop() {
    this.webService.stop();
  }
}
