package titan.ccp.configuration.api;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.DeleteResult;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.configuration.Config;
import titan.ccp.configuration.api.util.SensorAddedEvent;
import titan.ccp.configuration.api.util.SensorDeletedEvent;
import titan.ccp.configuration.api.util.SensorEvent;
import titan.ccp.configuration.api.util.SensorHierarchyComparatorUtils;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventPublisher;
import titan.ccp.configuration.events.KafkaPublisher;
import titan.ccp.configuration.events.NoopPublisher;
import titan.ccp.model.sensorregistry.AggregatedSensor;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;
import titan.ccp.model.sensorregistry.Sensor;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Wrapper for the database access for the sensor hierarchy.
 */
public final class SensorHierarchyRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(SensorHierarchyRepository.class);

  private static final String DATABASE_NAME = "sensor-management";
  private static final String COLLLECTION_NAME = "sensor-hierarchies";
  private static final String COLLECTION_SENSORS = "sensors";
  private static final String IDENTIFIER_FIELD = "identifier";
  private static final String TOP_LEVEL_IDENTIFIER_FIELD = "root";
  private static final String DEFAULT_HIERARCHY_IDENTIFIER = "default-hierarchy";

  private static final int ZERO = 0;

  private final EventPublisher eventPublisher;

  private final MongoClient mongoClient;

  private final MongoCollection<Document> sensorHierarchies;
  private final MongoCollection<Document> sensors;

  private final ClientSession session;

  /**
   * Create the repository.
   *
   * @throws ConfigurationRepositoryException When there occurs an error within the repository.
   */
  public SensorHierarchyRepository(final String host, final String port) {

    this.mongoClient = MongoClients.create(
        MongoClientSettings.builder()
            .applyToClusterSettings(
                builder -> {
                  builder.applyConnectionString(new ConnectionString(
                      "mongodb://" + host + ":" + port + "/"
                          + SensorHierarchyRepository.DATABASE_NAME
                          + "?replicaSet=rs0"));
                })
            .build());

    this.sensorHierarchies = this.mongoClient
        .getDatabase(DATABASE_NAME)
        .getCollection(COLLLECTION_NAME);
    this.sensors = this.mongoClient
        .getDatabase(DATABASE_NAME)
        .getCollection(COLLECTION_SENSORS);

    this.session = this.mongoClient.startSession();

    this.initDatabase();

    if (Config.EVENT_PUBLISHING) {
      this.eventPublisher = new KafkaPublisher(Config.KAFKA_BOOTSTRAP_SERVERS, Config.KAFKA_TOPIC);
    } else {
      this.eventPublisher = new NoopPublisher();
    }

    this.setDefaultSensorHierarchy();
  }

  /**
   * Create the repository.
   *
   * @throws ConfigurationRepositoryException When there occurs an error within the repository.
   */
  public SensorHierarchyRepository(final String host, final int port) {
    this(host, String.valueOf(port));
  }

  /**
   * Initialize the database.
   */
  private void initDatabase() {
    final IndexOptions indexOptions = new IndexOptions();
    indexOptions.unique(true);
    this.sensors
        .createIndex(Indexes.text(SensorHierarchyRepository.IDENTIFIER_FIELD), indexOptions);
    this.sensorHierarchies
        .createIndex(Indexes.text(SensorHierarchyRepository.IDENTIFIER_FIELD), indexOptions);
  }


  /**
   * Set the default sensor hierarchy.
   */
  private void setDefaultSensorHierarchy() {
    LOGGER.info("Set default sensor hierarchy");

    final SensorRegistry existingHierarchy =
        this.getSensorHierarchy(DEFAULT_HIERARCHY_IDENTIFIER);
    if (existingHierarchy == null) {
      LOGGER.info("Initial sensor hierarchy does not exist. Creating Hierarchy...");
      final SensorRegistry sensorHierarchy = this.getDefaultSensorHierarchy(); // NOPMD

      final Optional<List<String>> collisions = this.createSensorHierarchy(sensorHierarchy);
      if (collisions.isEmpty()) {
        LOGGER.info("Initial hierarchy created.");
      } else {
        LOGGER.info("Initial hierarchy already exists.");
      }

      // TODO emit more precise events
      this.eventPublisher.publish(Event.SENSOR_REGISTRY_STATUS, sensorHierarchy.toJson());
    } else {
      // TODO emit more precise events
      this.eventPublisher.publish(Event.SENSOR_REGISTRY_STATUS, existingHierarchy.toJson());
    }

    LOGGER.info("Initial hierarchy published.");
  }

  /**
   * Get the default sensor hierarchy.
   *
   * @return The default sensor hierarchy as JSON.
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
   * @return An empty sensor hierarchy with one aggregated sensor called
   *         {@link #DEFAULT_HIERARCHY_IDENTIFIER} and no children
   *
   */
  private SensorRegistry getEmptySensorHierarchy() {
    return new MutableSensorRegistry(DEFAULT_HIERARCHY_IDENTIFIER, "My Company");
  }


  /**
   * Stop the repository.
   */
  public void stop() {
    this.session.close();
    this.mongoClient.close();
  }


  /**
   * Get a sensor hierarchy for an identifier.
   *
   * @param identifier The identifier of the registry
   * @return Returns the sensor registry or null, if the registry is not found.
   */
  public SensorRegistry getSensorHierarchy(final String identifier) {
    final Document result = this.sensorHierarchies
        .find(Filters.eq(SensorHierarchyRepository.IDENTIFIER_FIELD, identifier))
        .first();

    return result == null
        ? null
        : SensorRegistry.fromJson(result.toJson());
  }

  /**
   * Get all sensor hierarchy that exist currently.
   *
   * @return A list of sensor hierarchies.
   */
  public List<SensorRegistry> getAllSensorHierarchies() {
    final MongoIterable<SensorRegistry> results = this.sensorHierarchies
        .find()
        .map(result -> SensorRegistry.fromJson(result.toJson()));
    return Lists.newArrayList(results);
  }

  /**
   * Create a sensor hierarchy.
   *
   * @param hierarchy The sensor hierarchy that should be created.
   * @return An empty Optional if the operation succeeded, else an Optional of a list of strings,
   *         representing the collided sensor identifiers.
   */
  public Optional<List<String>> createSensorHierarchy(final SensorRegistry hierarchy) {
    this.session.startTransaction();

    final List<String> globalCollisions = this
        .getSensorIdentifiersAccordingToFilter(
            Filters.or(this.buildIdFiltersForSensors(hierarchy)));
    if (!globalCollisions.isEmpty()) {
      this.session.abortTransaction();
      return Optional.of(globalCollisions);
    }

    final List<String> hierarchyCollisions = this.getCollisionsWithinHierarchy(hierarchy);
    if (!hierarchyCollisions.isEmpty()) {
      this.session.abortTransaction();
      return Optional.of(hierarchyCollisions);
    }

    this.sensors
        .insertMany(this.buildPairsOfSensorsAndHierarchy(hierarchy));
    this.sensorHierarchies.insertOne(Document.parse(hierarchy.toJson()));

    // TODO emit more precise events
    this.eventPublisher.publish(Event.SENSOR_REGISTRY_STATUS, hierarchy.toJson());

    this.session.commitTransaction();
    return Optional.empty();
  }

  /**
   * Get all sensors that already exist in the database. Two sensors are considered to be equal, if
   * their identifiers are equal. The operation filters all sensors in the database with respect
   * given filter.
   *
   * @param filter The filter used to filter the sensors.
   * @return A List of sensor identifiers for all sensors that match the filter.
   */
  private List<String> getSensorIdentifiersAccordingToFilter(final Bson filter) {
    final Iterator<Document> globalCollisions =
        this.sensors.find(filter).iterator();
    final List<String> acc = new ArrayList<>();
    while (globalCollisions.hasNext()) {
      final String collidedIdentifier =
          globalCollisions.next().getString(SensorHierarchyRepository.IDENTIFIER_FIELD);
      acc.add(collidedIdentifier);
    }

    return acc;
  }

  /**
   * Update a sensor hierarchy.
   *
   * @param hierarchy The sensor hierarchy that should be updated.
   * @return An empty Optional if the operation succeeded, else an Optional of a list of strings,
   *         representing the collided sensor identifiers.
   * @throws SensorHierarchyNotFoundException If the sensor hierarchy does not exist yet.
   */
  public Optional<List<String>> updateSensorHierarchy(final SensorRegistry hierarchy)
      throws SensorHierarchyNotFoundException {
    this.session.startTransaction();

    final SensorRegistry existingHierarchy =
        this.getSensorHierarchy(
            hierarchy.getTopLevelSensor().getIdentifier());

    if (existingHierarchy == null) {
      throw new SensorHierarchyNotFoundException();
    }

    final List<String> globalCollisions =
        this.getSensorIdentifiersAccordingToFilter(
            Filters.or(this.buildPairsOfSensorsAndHierarchyExcludingThisHierarchy(hierarchy)));

    if (!globalCollisions.isEmpty()) {
      this.session.abortTransaction();
      return Optional.of(globalCollisions);
    }

    final List<String> hierarchyCollisions = this.getCollisionsWithinHierarchy(hierarchy);
    if (!hierarchyCollisions.isEmpty()) {
      this.session.abortTransaction();
      return Optional.of(hierarchyCollisions);
    }

    final List<SensorEvent> comparisonResult =
        SensorHierarchyComparatorUtils.compareSensorHierarchies(existingHierarchy, hierarchy);

    for (final SensorEvent event : comparisonResult) {
      if (event instanceof SensorDeletedEvent) {
        final Document doc = new Document();
        doc.append(SensorHierarchyRepository.IDENTIFIER_FIELD, event.getSensor().getIdentifier());
        this.sensors.deleteOne(doc);
      } else if (event instanceof SensorAddedEvent) {
        final Document doc = new Document();
        doc.append(SensorHierarchyRepository.IDENTIFIER_FIELD, event.getSensor().getIdentifier());
        doc.append(SensorHierarchyRepository.TOP_LEVEL_IDENTIFIER_FIELD,
            hierarchy.getTopLevelSensor().getIdentifier());
        this.sensors.insertOne(doc);
      }
    }

    this.sensorHierarchies.replaceOne(
        Filters.eq(SensorHierarchyRepository.IDENTIFIER_FIELD,
            hierarchy.getTopLevelSensor().getIdentifier()),
        Document.parse(hierarchy.toJson()));

    // TODO emit more precise events
    this.eventPublisher.publish(Event.SENSOR_REGISTRY_CHANGED, hierarchy.toJson());

    this.session.commitTransaction();
    return Optional.empty();
  }

  /**
   * Build a list of Bson Documents that act as filter for all existing sensors that are in other
   * existing sensor hierarchies except in the given hierarchy.
   *
   * @param hierarchy The sensor hierarchy to exclude all sensor from.
   * @return A list of sensor identifiers matching the query.
   */
  private List<Bson> buildPairsOfSensorsAndHierarchyExcludingThisHierarchy(
      final SensorRegistry hierarchy) {
    final List<Document> pairs = this.buildPairsOfSensorsAndHierarchy(hierarchy);
    return pairs
        .stream()
        .map(document -> Filters.and(
            Filters.eq(SensorHierarchyRepository.IDENTIFIER_FIELD,
                document.getString(SensorHierarchyRepository.IDENTIFIER_FIELD)),
            Filters.not(
                Filters.eq(SensorHierarchyRepository.TOP_LEVEL_IDENTIFIER_FIELD,
                    document.getString(SensorHierarchyRepository.TOP_LEVEL_IDENTIFIER_FIELD)))))
        .collect(Collectors.toList());
  }

  /**
   * Get all colliding sensor identifiers within the hierarchy. Two sensors are colliding, iff they
   * have the same identifier.
   *
   * @param hierarchy The hierarchy.
   * @return The List of colliding identifiers.
   */
  private List<String> getCollisionsWithinHierarchy(final SensorRegistry hierarchy) {
    return hierarchy.getTopLevelSensor()
        .flatten()
        .stream()
        .filter(sensor -> {
          int count = 0;
          for (final Sensor sensor2 : hierarchy.flatten()) {
            if (sensor.getIdentifier().equals(sensor2.getIdentifier())) {
              count++;
            }
          }
          return count > 1;
        })
        .map(sensor -> sensor.getIdentifier())
        .collect(Collectors.toList());
  }


  /**
   * Build a list of BSON filters, containing key-value pairs for the identifiers for aggregated
   * sensors.
   *
   * @param hierarchy The hierarchy for which the pairs should be created,
   * @return A List of filters, where the key is {@link #IDENTIFIER_FIELD} and the value is the
   *         identifier of the respective aggregated sensor.
   */
  private List<Bson> buildIdFiltersForSensors(final SensorRegistry hierarchy) {
    return hierarchy
        .flatten()
        .stream()
        .filter(sensor -> sensor instanceof AggregatedSensor)
        .map(sensor -> Filters.eq(SensorHierarchyRepository.IDENTIFIER_FIELD,
            sensor.getIdentifier()))
        .collect(Collectors.toList());
  }

  /**
   * Build a list of documents, that represent entries for the collection specified by
   * {@link #IDENTIFIER_FIELD} and the other by # {@link #COLLECTION_SENSORS}.
   *
   * @param hierarchy The hierarchy to create the documents for.
   * @return The List of Documents, each consisting of two fields, the {@link #IDENTIFIER_FIELD} and
   *         the {@link #TOP_LEVEL_IDENTIFIER_FIELD} representing the identifier of the sensor
   *         identifier and the identifier of the top level sensor.
   */
  private List<Document> buildPairsOfSensorsAndHierarchy(
      final SensorRegistry hierarchy) {
    return hierarchy
        .flatten()
        .stream()
        .filter(sensor -> sensor instanceof AggregatedSensor)
        .map(sensor -> {
          final Document doc = new Document();
          doc.append(SensorHierarchyRepository.IDENTIFIER_FIELD, sensor.getIdentifier());
          doc.append(SensorHierarchyRepository.TOP_LEVEL_IDENTIFIER_FIELD,
              hierarchy.getTopLevelSensor().getIdentifier());
          return doc;
        })
        .collect(Collectors.toList());
  }

  /**
   * Delete a sensor hierarchy by identifier.
   *
   * @param identifier The identifier for the hierarchy that should be deleted.
   * @throws SensorHierarchyNotFoundException If the hierarchy that should be deleted is not found.
   */
  public void deleteSensorHierarchy(final String identifier)
      throws SensorHierarchyNotFoundException {
    final DeleteResult result = this.sensorHierarchies
        .deleteOne(Filters.eq(SensorHierarchyRepository.IDENTIFIER_FIELD, identifier));
    if (result.getDeletedCount() > ZERO) {
      throw new SensorHierarchyNotFoundException();
    }

  }

  /**
   * Exception indicating the hierarchy does not exist in the database.
   */
  @SuppressWarnings("serial")
  public class SensorHierarchyNotFoundException extends Exception {

  }
}
