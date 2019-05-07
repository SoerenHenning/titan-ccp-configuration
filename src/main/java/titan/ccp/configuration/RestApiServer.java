package titan.ccp.configuration;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Service;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventPublisher;
import titan.ccp.configuration.events.KafkaPublisher;
import titan.ccp.configuration.events.NoopPublisher;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;

public final class RestApiServer {

  private final Config config = Config.create();

  private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);
  private final Service webService;
  private final boolean enableCors;

  private static final String SENSOR_REGISTRY_PATH = "/sensor-registry";

  private static final String ACCESS_FORBIDDEN_MESSAGE = "Access forbidden";

  private static final String INTERNAL_SERVER_ERROR_MESSAGE = "Internal Server Error";

  private final EventPublisher eventPublisher;

  private final ConfigurationRepository configurationRepository = new ConfigurationRepository();

  private RetryPolicy<Object> jedisRetryPolicy;


  /**
   * Creates a new webserver.
   */
  public RestApiServer(final int port, final boolean enableCors) {
    LOGGER.info("Instantiating API server");
    this.webService = Service.ignite().port(port);
    this.enableCors = enableCors;

    // setup event publishing
    if (this.config.EVENT_PUBLISHING) {
      this.eventPublisher =
          new KafkaPublisher(this.config.KAFKA_BOOTSTRAP_SERVERS, this.config.KAFKA_TOPIC);
    } else {
      this.eventPublisher = new NoopPublisher();
    }

    // load default sensor registry
    this.setDefaultSensorRegistry();
  }


  private void setDefaultSensorRegistry() {
    final boolean isDemo = this.config.DEMO; // NOCS
    final String sensorRegistry =
        isDemo ? this.getDemoSensorRegistry() : this.getEmptySensorRegistry();
    this.configurationRepository.putConfigurationSafe(sensorRegistry);
    LOGGER.info("Set sensor registry");
  }

  private String getDemoSensorRegistry() {
    try {
      final URL url = Resources.getResource("demo_sensor_registry.json");
      return Resources.toString(url, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private String getEmptySensorRegistry() {
    return new MutableSensorRegistry("root").toJson();
  }

  public void start() {
    LOGGER.info("Starting API server");

    if (this.enableCors) {
      this.enableCors();
    }

    this.webService.get(SENSOR_REGISTRY_PATH, (request, response) -> {
      // TODO try catch
      return this.configurationRepository.getConfiguration();
    });

    this.webService.put(SENSOR_REGISTRY_PATH, (request, response) -> {
      if (this.config.DEMO) {
        response.status(403); // NOCS HTTP response code
        return ACCESS_FORBIDDEN_MESSAGE;
      }

      // TODO validation
      final SensorRegistry sensorRegistry = SensorRegistry.fromJson(request.body());
      final String json = sensorRegistry.toJson();

      // TODO try catch
      final String repositoryResponse = this.configurationRepository.putConfiguration(json);
      if ("OK".equals(repositoryResponse)) {
        this.eventPublisher.publish(Event.SENSOR_REGISTRY_CHANGED, json);
        response.status(204); // NOCS HTTP response code
        return "";
      } else {
        response.status(500); // NOCS HTTP response code
        return INTERNAL_SERVER_ERROR_MESSAGE;
      }
    });

  }

  private void enableCors() {

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
