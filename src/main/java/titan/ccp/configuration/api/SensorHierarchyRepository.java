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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.configuration.Config;
import titan.ccp.configuration.api.util.EventType;
import titan.ccp.configuration.api.util.SensorChangedEvent;
import titan.ccp.configuration.api.util.SensorHierarchyComparatorUtils;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventPublisher;
import titan.ccp.configuration.events.KafkaPublisher;
import titan.ccp.configuration.events.NoopPublisher;
import titan.ccp.model.sensorregistry.AggregatedSensor;
import titan.ccp.model.sensorregistry.MachineSensor;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;
import titan.ccp.model.sensorregistry.Sensor;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Wrapper for the database access for the sensor hierarchy.
 */
public final class SensorHierarchyRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(SensorHierarchyRepository.class);

  private static final String DATABASE_NAME = "sensorManagement";
  private static final String COLLLECTION_NAME = "sensorHierarchies";
  private static final String COLLECTION_SENSORS = "sensorGroups";
  private static final String COLLECTION_MACHINE_SENSORS = "machineSensors";
  private static final String IDENTIFIER_FIELD = "identifier";
  private static final String PARENT_FIELD = "parent";
  private static final String TOP_LEVEL_IDENTIFIER_FIELD = "topLevelSensor";
  private static final String DEFAULT_HIERARCHY_IDENTIFIER = "root";

  private static final int ZERO = 0;
  private static final int ONE = 1;

  private final EventPublisher eventPublisher;

  private final MongoClient mongoClient;

  /**
   * Schema consists of the properties of the sensor hierarchy as json where the index is
   * (IDENTIFIER_FIELD).
   */
  private final MongoCollection<Document> sensorHierarchies;

  /**
   * Schema: (IDENTIFIER_FIELD, TOP_LEVEL_IDENTIFIER_FIELD, PARENT_FIELD) where the index is
   * (IDENTIFIER_FIELD, TOP_LEVEL_IDENTIFIER).
   */
  private final MongoCollection<Document> sensorGroups;

  /**
   * Schema: (IDENTIFIER_FIELD, TOP_LEVEL_IDENTIFIER_FIELD, PARENT_FIELD) where the index is
   * (IDENTIFIER_FIELD).
   */
  private final MongoCollection<Document> machineSensors;

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

    this.machineSensors = this.mongoClient
        .getDatabase(DATABASE_NAME)
        .getCollection(COLLECTION_MACHINE_SENSORS);
    this.sensorGroups = this.mongoClient
        .getDatabase(DATABASE_NAME)
        .getCollection(COLLECTION_SENSORS);
    this.sensorHierarchies = this.mongoClient
        .getDatabase(DATABASE_NAME)
        .getCollection(COLLLECTION_NAME);

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
    this.machineSensors
        .createIndex(Indexes.compoundIndex(Indexes.text(IDENTIFIER_FIELD),
            Indexes.text(TOP_LEVEL_IDENTIFIER_FIELD)));
    this.sensorGroups
        .createIndex(Indexes.text(SensorHierarchyRepository.IDENTIFIER_FIELD), indexOptions);
    this.sensorHierarchies
        .createIndex(Indexes.text(SensorHierarchyRepository.IDENTIFIER_FIELD), indexOptions);
  }


  /**
   * Set the default sensor hierarchy.
   */
  private void setDefaultSensorHierarchy() {
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
      LOGGER.info("Default sensor hierarchy already exists.");
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
      final SensorRegistry initial = SensorRegistry.fromJson(Config.INITIAL_SENSOR_HIERARCHY);
      if (initial == null) {
        return this.getEmptySensorHierarchy();
      } else {
        return initial;
      }
    }
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

    final List<String> globalSensorGroupsCollisions =
        this.getSensorGroupIdentifiersAccordingToFilter(
            Filters.or(this.buildIdFiltersForSensors(hierarchy)));
    if (!globalSensorGroupsCollisions.isEmpty()) {
      this.session.abortTransaction();
      return Optional.of(globalSensorGroupsCollisions);
    }

    final List<String> hierarchyCollisions = this.getCollisionsWithinHierarchy(hierarchy);
    if (!hierarchyCollisions.isEmpty()) {
      this.session.abortTransaction();
      return Optional.of(hierarchyCollisions);
    }

    this.updateSensorCollectionsOnCreate(hierarchy);

    this.sensorHierarchies.insertOne(Document.parse(hierarchy.toJson()));

    this.session.commitTransaction();

    final List<SensorChangedEvent> comparisonResult = hierarchy.flatten()
        .stream()
        .map(sensor -> new SensorChangedEvent(sensor, EventType.SENSOR_ADDED))
        .collect(Collectors.toList());

    this.emitSensorChangedEvents(hierarchy, comparisonResult);

    return Optional.empty();
  }

  /**
   * Update the collections {@link #sensorGroups} and {@link #machineSensors} when a sensor
   * hierarchy is created.
   *
   * @param comparisonResult The comparison result of the old and new hierarchy.
   * @param existingHierarchy The old sensor hierarchy.
   */
  private void updateSensorCollectionsOnCreate(final SensorRegistry hierarchy) {
    this.sensorGroups
        .insertMany(this.buildSensorGroupDocuments(hierarchy));
    final List<Document> machineSensors = this.buildMachineSensorDocuments(hierarchy);
    if (!machineSensors.isEmpty()) {
      this.machineSensors.insertMany(machineSensors);
    }
  }

  /**
   * Get all sensor groups that already exist in the database. Two sensor groups are considered to
   * be equal, if their identifiers are equal. The operation filters all sensor groups in the
   * database with respect to the given filter.
   *
   * @param filter The filter used to filter the sensor-groups.
   * @return A List of sensor identifiers for all sensor groups that match the filter.
   */
  private List<String> getSensorGroupIdentifiersAccordingToFilter(final Bson filter) {
    return this.sensorGroups.find(filter)
        .into(new LinkedList<Document>())
        .stream()
        .map(document -> document.getString(SensorHierarchyRepository.IDENTIFIER_FIELD))
        .collect(Collectors.toList());
  }

  // Only needed if precise events are emitted
  /**
   * Get all machine sensors that already exist in the database. Two machine sensors are considered
   * to be equal, if their identifiers are equal. The operation filters all machine sensors in the
   * database with respect to the given filter.
   *
   * @param filter The filter used to filter the machine sensors.
   * @return A List of sensor identifiers for all machine-sensors that match the filter.
   */
  private List<String> getMachineSensorsIdentifiersAccordingToFilter(final Bson filter) {
    return this.machineSensors.find(filter)
        .into(new LinkedList<Document>())
        .stream()
        .map(document -> document.getString(SensorHierarchyRepository.IDENTIFIER_FIELD))
        .collect(Collectors.toList());
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
        this.getSensorHierarchy(hierarchy.getTopLevelSensor().getIdentifier());

    if (existingHierarchy == null) {
      throw new SensorHierarchyNotFoundException();
    }

    final List<String> globalCollisions =
        this.getSensorGroupIdentifiersAccordingToFilter(
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

    final List<SensorChangedEvent> comparisonResult =
        SensorHierarchyComparatorUtils.compareSensorHierarchies(existingHierarchy, hierarchy);

    this.updateSensorCollectionsOnUpdate(comparisonResult, existingHierarchy);

    this.sensorHierarchies.replaceOne(Filters.eq(SensorHierarchyRepository.IDENTIFIER_FIELD,
        hierarchy.getTopLevelSensor().getIdentifier()), Document.parse(hierarchy.toJson()));

    this.session.commitTransaction();

    this.emitSensorChangedEvents(hierarchy, comparisonResult);

    return Optional.empty();
  }

  /**
   * Update the collections {@link #sensorGroups} and {@link #machineSensors} when a sensor
   * hierarchy is updated.
   *
   * @param comparisonResult The comparison result of the old and new hierarchy.
   * @param existingHierarchy The old sensor hierarchy.
   */
  private void updateSensorCollectionsOnUpdate(final List<SensorChangedEvent> comparisonResult,
      final SensorRegistry existingHierarchy) {
    for (final SensorChangedEvent event : comparisonResult) {
      if (event.getEventType() == EventType.SENSOR_ADDED) {
        final Document document = this.buildSensorDocument(event.getSensor(), existingHierarchy);
        if (event.getSensor() instanceof AggregatedSensor) {
          // aggregated sensor
          this.sensorGroups.insertOne(document);
        } else {
          // machine sensor
          this.machineSensors.insertOne(document);
        }
      } else if (event.getEventType() == EventType.SENSOR_DELETED) {
        final Document document = this.buildSensorDocument(event.getSensor(), existingHierarchy);

        if (event.getSensor() instanceof AggregatedSensor) {
          // aggregated sensor
          this.sensorGroups.deleteOne(document);
        } else {
          // machine sensor
          this.machineSensors.deleteOne(document);
        }
      } else if (event.getEventType() == EventType.SENSOR_MOVED) {
        final Document document = this.buildSensorDocument(event.getSensor(), existingHierarchy);

        // update sensor in db and publish
        if (event.getSensor() instanceof AggregatedSensor) {
          // aggregated sensor
          this.sensorGroups.replaceOne(
              Filters.eq(IDENTIFIER_FIELD, event.getSensor().getIdentifier()), document);
        } else {
          // machine sensor
          final Bson filter = Filters.and(
              Filters.eq(IDENTIFIER_FIELD, event.getSensor().getIdentifier()),
              Filters.eq(TOP_LEVEL_IDENTIFIER_FIELD,
                  existingHierarchy.getTopLevelSensor().getIdentifier()));
          this.machineSensors.replaceOne(filter, document);
        }
      }
    }
  }

  /**
   * Emit the information concerning the changes of the sensors of a hierarchy.
   *
   * @param hierarchy The hierarchy (used until more fine grained events are used by other services)
   * @param comparisonResult A list of events, representing the changes of the sensors.
   */
  private void emitSensorChangedEvents(final SensorRegistry hierarchy,
      final List<SensorChangedEvent> comparisonResult) {
    // TODO remove when emitting more precise events
    this.eventPublisher.publish(Event.SENSOR_REGISTRY_CHANGED, hierarchy.toJson());

    // This is how precise events could be emitted
    comparisonResult.forEach(event -> {
      if (event.getSensor() instanceof AggregatedSensor) {
        // TODO emit event with format (sensorIdentifier, [parentIdentifier])
        final AggregatedSensor parent = event.getSensor().getParent().orElse(null);
        final String msg = "Event: '" + event.getSensor().getIdentifier()
            + "' (Sensor Group) | Parent: " + (parent == null ? "none" : parent.getIdentifier());
        LOGGER.info(msg);
      } else {
        // machine sensor
        final List<String> parents = this.getMachineSensorsIdentifiersAccordingToFilter(
            Filters.eq(IDENTIFIER_FIELD, event.getSensor().getIdentifier()));
        // TODO emit event with format (sensorIdentifier, parentIdentifiers[])
        final String msg = "Event: '" + event.getSensor().getIdentifier()
            + "' (Machine Sensor) | Parents: ["
            + parents.stream().reduce((acc, next) -> acc + ", " + next).get() + "]";
        LOGGER.info(msg);
      }
    });
  }

  /**
   * Build a BSON document, representing a sensor in the MongoDB collections {@link #sensorGroups}
   * or {@link #machineSensors}.
   *
   * @param sensor The sensor that should be represented.
   * @param hierarchy The hierarchy the sensor is contained in.
   * @return The respective BSON document representing the sensor in the hierarchy.
   */
  private Document buildSensorDocument(final Sensor sensor, final SensorRegistry hierarchy) {
    final Document doc = new Document();
    doc.append(SensorHierarchyRepository.IDENTIFIER_FIELD, sensor.getIdentifier());
    doc.append(SensorHierarchyRepository.TOP_LEVEL_IDENTIFIER_FIELD,
        hierarchy.getTopLevelSensor().getIdentifier());
    final String parentIdentifier =
        sensor.getIdentifier().equals(hierarchy.getTopLevelSensor().getIdentifier()) ? null
            : sensor.getParent().get().getIdentifier();
    doc.append(SensorHierarchyRepository.PARENT_FIELD, parentIdentifier);
    return doc;
  }

  /**
   * Build a list of Bson Documents that act as filter for all existing sensorGroups that are in
   * other existing sensor hierarchies except in the given hierarchy.
   *
   * @param hierarchy The sensor hierarchy to exclude all sensor from.
   * @return A list of sensor identifiers matching the query.
   */
  private List<Bson> buildPairsOfSensorsAndHierarchyExcludingThisHierarchy(
      final SensorRegistry hierarchy) {
    final List<Document> pairs = this.buildSensorGroupDocuments(hierarchy);
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
   * Get all colliding sensor identifiers within the hierarchy. Two sensorGroups are colliding, iff
   * they have the same identifier.
   *
   * @param hierarchy The hierarchy.
   * @return The List of colliding identifiers.
   */
  // TODO fix bug due to naive comparison
  private List<String> getCollisionsWithinHierarchy(final SensorRegistry hierarchy) {
    final Collection<Sensor> flattedHierarchy = hierarchy.flatten();
    return flattedHierarchy.stream()
        .filter(sensor -> flattedHierarchy
            .stream()
            .filter(sensor2 -> sensor.getIdentifier().equals(sensor2.getIdentifier()))
            .count() > ONE)
        .map(sensor -> sensor.getIdentifier())
        .collect(Collectors.toList());
  }


  /**
   * Build a list of BSON filters, containing key-value pairs for the identifiers for aggregated
   * sensorGroups.
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
  private List<Document> buildSensorGroupDocuments(
      final SensorRegistry hierarchy) {
    return hierarchy
        .flatten()
        .stream()
        .filter(sensor -> sensor instanceof AggregatedSensor)
        .map(sensor -> this.buildSensorDocument(sensor, hierarchy))
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
  private List<Document> buildMachineSensorDocuments(
      final SensorRegistry hierarchy) {
    return hierarchy
        .flatten()
        .stream()
        .filter(sensor -> sensor instanceof MachineSensor)
        .map(sensor -> {
          final Document doc = new Document();
          doc.append(SensorHierarchyRepository.IDENTIFIER_FIELD, sensor.getIdentifier());
          doc.append(SensorHierarchyRepository.TOP_LEVEL_IDENTIFIER_FIELD,
              hierarchy.getTopLevelSensor().getIdentifier());
          final Sensor parent = sensor.getParent().orElse(null);
          final String parentIdentifier = PARENT_FIELD == null ? null : parent.getIdentifier();
          doc.append(SensorHierarchyRepository.PARENT_FIELD, parentIdentifier);
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
