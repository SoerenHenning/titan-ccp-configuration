package titan.ccp.configuration.api.util;

import titan.ccp.model.sensorregistry.Sensor;

/**
 * Class that represents changing a sensor within a hierarchy.
 */
public class SensorChangedEvent extends SensorEvent {

  public SensorChangedEvent(final Sensor sensor) {
    super(sensor);
    // TODO Auto-generated constructor stub
  }

}
