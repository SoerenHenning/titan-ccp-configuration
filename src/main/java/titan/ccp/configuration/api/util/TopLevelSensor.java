package titan.ccp.configuration.api.util;

/**
 * Class for automatic GSON deserialization for the sensor-registries, that are identified by their
 * toplevel-sensor.
 */
public class TopLevelSensor {
  private final String identifier;
  private final String name;

  public TopLevelSensor(final String identifier, final String name) {
    this.identifier = identifier;
    this.name = name;
  }
}
