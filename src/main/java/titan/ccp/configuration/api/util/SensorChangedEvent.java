package titan.ccp.configuration.api.util;

import titan.ccp.model.sensorregistry.Sensor;

/**
 * Class that represents a modification of a sensor with respect to a hierarchy.
 */
public class SensorChangedEvent {

  private final Sensor sensor;
  private final EventType eventType;

  public SensorChangedEvent(final Sensor sensor, final EventType eventType) {
    this.sensor = sensor;
    this.eventType = eventType;
  }

  public Sensor getSensor() {
    return this.sensor;
  }

  public EventType getEventType() {
    return this.eventType;
  }
}
