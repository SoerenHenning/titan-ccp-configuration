package titan.ccp.configuration;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Service;
import titan.ccp.configuration.ConfigurationRepository.ConfigurationRepositoryException;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Webserver for the Configuration microservice.
 */
public final class RestApiServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

  private static final String ACCESS_FORBIDDEN_MESSAGE = "Access forbidden";

  private static final String INTERNAL_SERVER_ERROR_MESSAGE = "Internal Server Error";

  private static final String SENSOR_REGISTRY_PATH = "/sensor-registry";

  private final ConfigurationRepository configurationRepository = new ConfigurationRepository();

  private final Service webService;

  private final boolean enableCors;

  /**
   * Creates a new webserver.
   */
  public RestApiServer(final int port, final boolean enableCors) {
    LOGGER.info("Instantiating API server");
    this.webService = Service.ignite().port(port);
    this.enableCors = enableCors;

    // load default sensor registry
    this.setDefaultSensorRegistry();
  }


  /**
   * Set the sensor-registry to default.
   */
  private void setDefaultSensorRegistry() {
    final boolean isDemo = Config.DEMO;
    final String sensorRegistry =
        isDemo ? this.getDemoSensorRegistry() : this.getEmptySensorRegistry();
    this.configurationRepository.setInitialConfiguration(sensorRegistry);
    LOGGER.info("Set sensor registry");
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
   * Starts the service-api.
   */
  public void start() {
    LOGGER.info("Starting API server");

    if (this.enableCors) {
      this.enableCorsHeaders();
    }

    this.webService.get(SENSOR_REGISTRY_PATH, (request, response) -> {
      try {
        return this.configurationRepository.getConfiguration();
      } catch (final ConfigurationRepositoryException e) {
        response.status(500); // NOCS HTTP response code
        return INTERNAL_SERVER_ERROR_MESSAGE;
      }
    });

    this.webService.put(SENSOR_REGISTRY_PATH, (request, response) -> {
      if (Config.DEMO) {
        response.status(403); // NOCS HTTP response code
        return ACCESS_FORBIDDEN_MESSAGE;
      }

      // TODO validation
      final SensorRegistry sensorRegistry = SensorRegistry.fromJson(request.body());
      final String json = sensorRegistry.toJson();

      try {
        this.configurationRepository.changeConfiguration(json);
      } catch (final ConfigurationRepositoryException e) {
        response.status(500); // NOCS HTTP response code
        return INTERNAL_SERVER_ERROR_MESSAGE;
      }

      response.status(204); // NOCS HTTP response code
      return "";
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



}
