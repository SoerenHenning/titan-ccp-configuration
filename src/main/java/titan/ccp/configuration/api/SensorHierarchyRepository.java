package titan.ccp.configuration.api;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.DeleteResult;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.configuration.Config;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventPublisher;
import titan.ccp.configuration.events.KafkaPublisher;
import titan.ccp.configuration.events.NoopPublisher;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Wrapper for the database access for the sensor hierarchy.
 */
public final class SensorHierarchyRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(SensorHierarchyRepository.class);

  private final EventPublisher eventPublisher;

  private static final String DATABASE_NAME = "sensor-management";

  private static final String COLLLECTION_NAME = "sensor-hierarchies";

  private static final String IDENTIFIER_FIELD = "identifier";


  private final MongoClient mongoClient = MongoClients.create(
      MongoClientSettings.builder()
          .applyToClusterSettings(
              builder -> builder.hosts(
                  Arrays.asList(new ServerAddress(Config.MONGODB_HOST, Config.MONGODB_PORT))))
          .build());

  /**
   * Create the repository.
   *
   * @throws ConfigurationRepositoryException When there occurs an error within the repository.
   */
  public SensorHierarchyRepository() {

    this.init();

    // setup event publishing
    if (Config.EVENT_PUBLISHING) {
      this.eventPublisher = new KafkaPublisher(Config.KAFKA_BOOTSTRAP_SERVERS, Config.KAFKA_TOPIC);
    } else {
      this.eventPublisher = new NoopPublisher();
    }

    this.setDefaultSensorHierarchy();

  }

  private void init() {
    final IndexOptions indexOptions = new IndexOptions();
    indexOptions.unique(true);
    this.mongoClient
        .getDatabase(SensorHierarchyRepository.DATABASE_NAME)
        .getCollection(SensorHierarchyRepository.COLLLECTION_NAME)
        .createIndex(Indexes.text(IDENTIFIER_FIELD), indexOptions);
  }


  /**
   * Set initial configuration to the database. The operation is encapsulated and does not throw an
   * exception.
   *
   * @throws ConfigurationRepositoryException When the initial sensor hierarchy cannot be put to the
   *         repository.
   */
  private void setDefaultSensorHierarchy() {
    LOGGER.info("Set default sensor hierarchy");

    final SensorRegistry sensorHierarchy = this.getDefaultSensorHierarchy(); // NOPMD

    try {
      this.createSensorHierarchy(sensorHierarchy);
      this.publishHierarchy(sensorHierarchy, Event.SENSOR_REGISTRY_STATUS);
    } catch (SensorHierarchyExistsException | SensorHierarchyRepositoryException e) {
      LOGGER.error("Could not load default sensor registry", e);
    }
  }

  /**
   * Get the default sensor hierarchy.
   *
   * @return The default sensor hierarchy as JSON
   */
  private SensorRegistry getDefaultSensorHierarchy() {
    if (Config.DEMO) {
      return this.getDemoSensorHierarchy();
    } else {
      final SensorRegistry initial = this.getInitialSensorHierarchy();
      if (initial == null) {
        return this.getEmptySensorHierarchy();
      } else {
        return initial;
      }
    }
  }

  /**
   * Get the inital sensor hierarchy.
   *
   * @return The inital sensor hierarchy as JSON
   */
  private SensorRegistry getInitialSensorHierarchy() {
    return SensorRegistry.fromJson(Config.INITIAL_SENSOR_HIERARCHY);
  }


  /**
   * Get the demo sensor hierarchy.
   *
   * @return The demo sensor hierarchy
   */
  private SensorRegistry getDemoSensorHierarchy() {
    try {
      final URL url = Resources.getResource("demo_sensor_hierarchy.json");
      return SensorRegistry.fromJson(Resources.toString(url, StandardCharsets.UTF_8));
    } catch (final IOException e) {
      LOGGER.error("Demo sensor hierarchy could not be loaded", e);
      throw new IllegalStateException(e);
    }
  }

  /**
   * Get the empty sensor hierarchy.
   *
   * @return An empty sensor hierarchy with one aggregated sensor called "root" and no children
   *
   */
  private SensorRegistry getEmptySensorHierarchy() {
    return new MutableSensorRegistry("root", "My Company");
  }


  /**
   * Close the repository.
   */
  public void close() {
    this.mongoClient.close();
  }


  public SensorRegistry getSensorHierarchy(final String identifier) {
    final Document result = this.mongoClient
        .getDatabase(SensorHierarchyRepository.DATABASE_NAME)
        .getCollection(SensorHierarchyRepository.COLLLECTION_NAME)
        .find(Filters.eq(IDENTIFIER_FIELD, identifier))
        .first();

    return result != null
        ? SensorRegistry.fromJson(result.toJson())
        : null;
  }

  public List<SensorRegistry> getAllSensorHierarchies() {
    final MongoIterable<SensorRegistry> results = this.mongoClient
        .getDatabase(SensorHierarchyRepository.DATABASE_NAME)
        .getCollection(SensorHierarchyRepository.COLLLECTION_NAME)
        .find()
        .map(result -> SensorRegistry.fromJson(result.toJson()));
    return Lists.newArrayList(results);
  }

  public void createSensorHierarchy(final SensorRegistry hierarchy)
      throws SensorHierarchyExistsException, SensorHierarchyRepositoryException {
    final Document document = Document.parse(hierarchy.toJson());
    try {
      this.mongoClient
          .getDatabase(SensorHierarchyRepository.DATABASE_NAME)
          .getCollection(SensorHierarchyRepository.COLLLECTION_NAME)
          .insertOne(document);
    }
    // handle write-related errors
    catch (final MongoWriteException e) {
      // if duplicate index -> hierarchy already exists
      if (e.getError().getCode() == 11000) {
        throw new SensorHierarchyExistsException();
      }
      // handle other errors
      else {
        throw new SensorHierarchyRepositoryException();
      }
    }
    // handle non-write-related errors
    catch (final Exception e) {
      throw new SensorHierarchyRepositoryException();
    }
  }

  public void updateSensorHierarchy(final SensorRegistry hierarchy)
      throws SensorHierarchyNotFoundException {
    final Document updatedDocument = Document.parse(hierarchy.toJson());
    final Document result = this.mongoClient
        .getDatabase(SensorHierarchyRepository.DATABASE_NAME)
        .getCollection(SensorHierarchyRepository.COLLLECTION_NAME)
        .findOneAndReplace(
            Filters.eq(IDENTIFIER_FIELD, hierarchy.getTopLevelSensor().getIdentifier()),
            updatedDocument);
    if (result == null) {
      throw new SensorHierarchyNotFoundException();
    }
    this.publishHierarchy(hierarchy, Event.SENSOR_REGISTRY_CHANGED);
  }

  private void publishHierarchy(final SensorRegistry hierarchy, final Event type) {
    if (hierarchy.getTopLevelSensor().getIdentifier().equals("root")) { // tmp only root sensor
      this.eventPublisher.publish(type, hierarchy.toJson());
    }
  }

  public void deleteSensorHierarchy(final String identifier)
      throws SensorHierarchyNotFoundException, SensorHierarchyRepositoryException {
    try {
      final DeleteResult result = this.mongoClient
          .getDatabase(SensorHierarchyRepository.DATABASE_NAME)
          .getCollection(SensorHierarchyRepository.COLLLECTION_NAME)
          .deleteOne(Filters.eq(IDENTIFIER_FIELD, identifier));
      if (result.getDeletedCount() < 1) {
        throw new SensorHierarchyNotFoundException();
      }
    } catch (final Exception e) {
      throw new SensorHierarchyRepositoryException();
    }
  }

  @SuppressWarnings("serial")
  public class SensorHierarchyRepositoryException extends Exception {

  }
  @SuppressWarnings("serial")
  public class SensorHierarchyNotFoundException extends Exception {

  }
  @SuppressWarnings("serial")
  public class SensorHierarchyExistsException extends Exception {

  }
}
