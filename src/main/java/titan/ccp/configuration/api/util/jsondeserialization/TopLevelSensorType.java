package titan.ccp.configuration.api.util.jsondeserialization;

/**
 * Class for automatic GSON deserialization for the sensor hierarchies, that are identified by their
 * toplevel sensor.
 */
@SuppressWarnings("PMD")
public class TopLevelSensorType {
  private final String identifier;
  private final String name;

  public TopLevelSensorType(final String identifier, final String name) {
    this.identifier = identifier;
    this.name = name;
  }
}
