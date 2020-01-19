package titan.ccp.configuration.api.util;

import titan.ccp.model.sensorregistry.Sensor;

/**
 * Class that represents a modification of a sensor with respect to a hierarchy.
 */
public abstract class SensorEvent {

  private final Sensor sensor;

  public SensorEvent(final Sensor sensor) {
    this.sensor = sensor;
  }

  public Sensor getSensor() {
    return this.sensor;
  }
}
