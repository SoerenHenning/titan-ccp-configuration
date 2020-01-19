package titan.ccp.configuration.api.util;

import titan.ccp.model.sensorregistry.Sensor;

/**
 * Class that represents deleting a sensor from a hierarchy.
 */
public class SensorDeletedEvent extends SensorEvent {

  public SensorDeletedEvent(final Sensor sensor) {
    super(sensor);
    // TODO Auto-generated constructor stub
  }

}
