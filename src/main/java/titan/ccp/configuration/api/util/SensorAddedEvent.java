package titan.ccp.configuration.api.util;

import titan.ccp.model.sensorregistry.Sensor;

/**
 * Class that represents adding a sensor to a hierarchy.
 */
public class SensorAddedEvent extends SensorEvent {

  public SensorAddedEvent(final Sensor sensor) {
    super(sensor);
    // TODO Auto-generated constructor stub
  }

}
