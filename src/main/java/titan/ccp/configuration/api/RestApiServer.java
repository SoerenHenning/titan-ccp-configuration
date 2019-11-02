package titan.ccp.configuration.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Service;
import titan.ccp.configuration.Config;
import titan.ccp.configuration.api.SensorHierarchyRepository.SensorHierarchyExistsException;
import titan.ccp.configuration.api.SensorHierarchyRepository.SensorHierarchyNotFoundException;
import titan.ccp.configuration.api.SensorHierarchyRepository.SensorHierarchyRepositoryException;
import titan.ccp.configuration.api.util.TopLevelSensor;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Webserver for the Configuration microservice.
 */
public final class RestApiServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

  private static final String ACCESS_FORBIDDEN_MESSAGE = "Access forbidden";
  private static final String INTERNAL_SERVER_ERROR_MESSAGE = "Internal Server Error";
  private static final String NOT_FOUND_ERROR_MESSAGE = "Resource not found";
  private static final String CONFLICT_ERROR_MESSAGE = "Resource already exists";

  private static final String GET_SENSOR_HIERARCHY_PATH = "/sensor-hierarchy/:id";
  private static final String POST_SENSOR_HIERARCHY_PATH = "/sensor-hierarchy";
  private static final String PUT_SENSOR_HIERARCHY_PATH = "/sensor-hierarchy/:id";
  private static final String DELETE_SENSOR_HIERARCHY_PATH = "/sensor-hierarchy/:id";
  private static final String GET_ALL_SENSOR_HIERARCHIES_PATH = "/sensor-hierarchy/";

  private static final Gson GSON = new GsonBuilder().create();


  private final SensorHierarchyRepository sensorHierarchyRepository;

  private final Service webService;

  private final boolean enableCors;

  /**
   * Creates a new webserver.
   */
  public RestApiServer(final int port, final boolean enableCors,
      final SensorHierarchyRepository sensorHierarchyRepository) {
    LOGGER.info("Instantiating API server");

    this.sensorHierarchyRepository = sensorHierarchyRepository;

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

    this.initializeRoutes();

    this.handleErrors();
  }

  /**
   * Initialize routes.
   */
  private void initializeRoutes() {

    // Get sensor hierarchy
    this.webService.get(GET_SENSOR_HIERARCHY_PATH, (request, response) -> {
      final String identifier = request.params("id");
      if (identifier == null) {
        response.status(400); // bad request;
        return "";
      }
      final SensorRegistry registry = this.sensorHierarchyRepository.getSensorHierarchy(identifier);
      if (registry == null) {
        response.status(400); // bad request
        return "";
      }
      return registry.toJson();
    });

    // Get all sensor hierarchies
    this.webService.get(GET_ALL_SENSOR_HIERARCHIES_PATH, (request, response) -> {
      return this.sensorHierarchyRepository
          .getAllSensorHierarchies()
          .stream()
          .map(registry -> new TopLevelSensor(
              registry.getTopLevelSensor().getIdentifier(),
              registry.getTopLevelSensor().getName()))
          .collect(Collectors.toList());
    }, RestApiServer.GSON::toJson);

    // Update sensor hierarchy
    this.webService.put(PUT_SENSOR_HIERARCHY_PATH, (request, response) -> {
      if (Config.DEMO) {
        response.status(403); // NOCS HTTP response code
        return ACCESS_FORBIDDEN_MESSAGE;
      }

      final String topLevelSensorIdentifier = request.params("id");
      final SensorRegistry sensorRegistry = SensorRegistry.fromJson(request.body());
      if (topLevelSensorIdentifier != null
          && topLevelSensorIdentifier.equals(sensorRegistry.getTopLevelSensor().getIdentifier())) {
        // TODO validate that hierarchy only contains existing machine sensors
        // TODO validate uniqueness of aggregated sensors within the hierarchy
        this.sensorHierarchyRepository.updateSensorHierarchy(sensorRegistry);

        response.status(200); // NOCS HTTP response code: OK
        return "OK";
      } else {
        response.status(400); // NOCS HTTP response code: Bad Request
      }

      return "";
    });

    // Create sensor hierarchy
    this.webService.post(POST_SENSOR_HIERARCHY_PATH, (request, response) -> {
      try {
        final SensorRegistry registry = SensorRegistry.fromJson(request.body());
        this.sensorHierarchyRepository.createSensorHierarchy(registry);
        response.status(204); // NOCS HTTP response code: Created
        return "OK";
      } catch (final JsonParseException e) {
        response.status(400); // NOCS HTTP response code: Bad Request
        return "";
      }
    });

    this.webService.delete(DELETE_SENSOR_HIERARCHY_PATH, (request, response) -> {
      final String identifier = request.params("id");
      this.sensorHierarchyRepository.deleteSensorHierarchy(identifier);
      return "OK";
    });
  }

  private void handleErrors() {
    // handle repository exceptions
    this.webService.exception(SensorHierarchyRepositoryException.class, (e, request, response) -> {
      response.status(500); // NOCS HTTP response code: Internal Server Error
      response.body(INTERNAL_SERVER_ERROR_MESSAGE);
    });
    this.webService.exception(SensorHierarchyNotFoundException.class, (e, request, response) -> {
      response.status(404); // NOCS HTTP response code: Not Found
      response.body(NOT_FOUND_ERROR_MESSAGE);
    });
    this.webService.exception(SensorHierarchyExistsException.class, (e, request, response) -> {
      response.status(409); // NOCS HTTP response code: Conflict
      response.body(CONFLICT_ERROR_MESSAGE);
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
