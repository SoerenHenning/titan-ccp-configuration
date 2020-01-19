package titan.ccp.configuration.api.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import titan.ccp.model.sensorregistry.Sensor;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Class for the comparison of sensor hierarchies.
 */
public final class SensorHierarchyComparatorUtils {

  private SensorHierarchyComparatorUtils() {

  }

  /**
   * Compare two sensor hierarchies.
   *
   * @param oldHierarchy The old hierarchy.
   * @param newHierarchy The new hierarchy.
   * @return A list of SensorEvent that contains the information which sensors are deleted from the
   *         old hierarchy, added with the new hierarchy.
   */
  public static List<SensorEvent> compareSensorHierarchies(final SensorRegistry oldHierarchy,
      final SensorRegistry newHierarchy) {

    final List<SensorEvent> acc = new ArrayList<>();

    final Collection<Sensor> oldHierarchySensors = oldHierarchy.flat();
    final Collection<Sensor> newHierarchySensors = newHierarchy.flat();

    // find deleted sensors
    for (final Sensor oldSensor : oldHierarchySensors) {
      boolean found = false; // NOPMD redefineable variable
      for (final Sensor newSensor : newHierarchySensors) {
        if (oldSensor.getIdentifier().equals(newSensor.getIdentifier())) {
          found = true;
          break;
        }
      }
      if (!found) {
        acc.add(new SensorDeletedEvent(oldSensor));
      }
    }

    // find added sensors
    for (final Sensor newSensor : newHierarchySensors) {
      boolean found = false; // NOPMD redefineable variable
      for (final Sensor oldSensor : oldHierarchySensors) {
        if (newSensor.getIdentifier().equals(oldSensor.getIdentifier())) {
          found = true;
          break;
        }
      }
      if (!found) {
        acc.add(new SensorAddedEvent(newSensor));
      }
    }

    // TODO check for changed sensors, according to the requirements of the aggregation service.
    return acc;
  }

}
