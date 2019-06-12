package titan.ccp.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.api.ConfigurationRepository;
import titan.ccp.api.ConfigurationRepository.ConfigurationRepositoryException;
import titan.ccp.api.RestApiServer;

/**
 * A microservice that manages the system-wide configuration. For example, the sensor registry. It
 * provides a REST interface to get or modify the configuration and optionally publishes changes to
 * a Kafka topic.
 *
 */
public final class ConfigurationService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationService.class);

  private static final String SHUTDOWN_SERVICE_MESSAGE = "Shutting down Configuration microservice";

  private RestApiServer webServer;

  private ConfigurationRepository configurationRepository;

  /**
   * Run the microservice.
   */
  public void run() {
    try {
      this.configurationRepository = new ConfigurationRepository();
    } catch (final ConfigurationRepositoryException e) {
      LOGGER.error("", e);
      this.stop();
    }

    this.webServer =
        new RestApiServer(Config.WEBSERVER_PORT, Config.CORS, this.configurationRepository);
    this.webServer.start();
  }

  public static void main(final String[] args) {
    new ConfigurationService().run();
  }

  /**
   * Stop the microservice.
   */
  private void stop() {
    LOGGER.warn(SHUTDOWN_SERVICE_MESSAGE);
    if (this.webServer != null) {
      this.webServer.stop();
    }
<<<<<<< HEAD
    if (this.configurationRepository != null) {
      this.configurationRepository.close();
=======

    Spark.port(this.config.getInt("webserver.port"));

    if (this.config.getBoolean("webserver.cors")) {
      Spark.options("/*", (request, response) -> {

        final String accessControlRequestHeaders =
            request.headers("Access-Control-Request-Headers");
        if (accessControlRequestHeaders != null) {
          response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
        }

        final String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
        if (accessControlRequestMethod != null) {
          response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
        }

        return "OK";
      });

      Spark.before((request, response) -> {
        response.header("Access-Control-Allow-Origin", "*");
      });
    }

    Spark.get(SENSOR_REGISTRY_PATH, (request, response) -> {
      final String redisResponse = this.jedis.get(REDIS_SENSOR_REGISTRY_KEY);
      if (redisResponse == null) {
        response.status(500); // NOCS HTTP response code
        return INTERNAL_SERVER_ERROR_MESSAGE;
      } else {
        return redisResponse;
      }
    });

    Spark.put(SENSOR_REGISTRY_PATH, (request, response) -> {
      if (this.config.getBoolean("demo")) { // NOCS
        response.status(403); // NOCS HTTP response code
        return ACCESS_FORBIDDEN_MESSAGE;
      } else {
        // TODO validation
        final SensorRegistry sensorRegistry = SensorRegistry.fromJson(request.body());
        final String json = sensorRegistry.toJson();
        final String redisResponse = this.jedis.set(REDIS_SENSOR_REGISTRY_KEY, json);
        if ("OK".equals(redisResponse)) {
          this.eventPublisher.publish(Event.SENSOR_REGISTRY_CHANGED, json);
          response.status(204); // NOCS HTTP response code
          return "";
        } else {
          response.status(500); // NOCS HTTP response code
          return INTERNAL_SERVER_ERROR_MESSAGE;
        }
      }
    });

    Spark.after((request, response) -> {
      response.type("application/json");
    });
  }

  public void stop() {
    this.jedis.close();
    this.eventPublisher.close();
  }

  private void setDefaultSensorRegistry() {
    // final boolean isDemo = this.config.getBoolean("demo"); // NOCS
    // this.getDefaultSensorRegsitry();
    final String sensorRegistry = this.getDefaultSensorRegsitry();
    // isDemo ? this.getDemoSensorRegsitry() : this.getEmptySensorRegsitry();
    this.jedis.set(REDIS_SENSOR_REGISTRY_KEY, sensorRegistry);
    LOGGER.info("Set sensor registry");
  }

  private String getDefaultSensorRegsitry() {
    if (this.config.getBoolean("demo")) { // NOCS
      return this.getDemoSensorRegsitry();
    } else {
      final String initial = this.getInitialSensorRegsitry();
      if (initial == null) {
        return this.getEmptySensorRegsitry();
      } else {
        return initial;
      }
    }
  }

  private String getDemoSensorRegsitry() {
    try {
      final URL url = Resources.getResource("demo_sensor_registry.json");
      return Resources.toString(url, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
>>>>>>> b319e2461e37eca992f48101e49603b41f1d9c49
    }
    System.exit(1); // NOPMD exit application manually
  }
<<<<<<< HEAD
=======

  private String getEmptySensorRegsitry() {
    return new MutableSensorRegistry("root").toJson();
  }

  private String getInitialSensorRegsitry() {
    return this.config.getString("initial.sensor.registry");
  }

  public static void main(final String[] args) {
    new ConfigurationService().start();
  }

>>>>>>> b319e2461e37eca992f48101e49603b41f1d9c49
}
