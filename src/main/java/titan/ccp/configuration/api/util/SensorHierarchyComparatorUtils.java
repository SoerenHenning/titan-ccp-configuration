package titan.ccp.configuration.api.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import titan.ccp.model.sensorregistry.AggregatedSensor;
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
   * @return A list of SensorChangedEvent that contains the information which sensors are deleted
   *         from the old hierarchy, added with the new hierarchy.
   */
  public static List<SensorChangedEvent> compareSensorHierarchies(final SensorRegistry oldHierarchy,
      final SensorRegistry newHierarchy) {

    final Collection<Sensor> oldHierarchySensors = oldHierarchy.flatten();
    final Collection<Sensor> newHierarchySensors = newHierarchy.flatten();

    final List<SensorChangedEvent> acc = new ArrayList<>();

    // find deleted sensors
    findDeletedSensors(oldHierarchySensors, newHierarchySensors, acc);

    // find added and moved sensors
    findAddedAndMovedSensors(oldHierarchySensors, newHierarchySensors, acc);

    return acc;
  }

  /**
   * Find all sensors that are contained in the old hierarchy, but not in the new hierarchy. All
   * sensors matching the criteria are put into the accumulator as a side effect.
   *
   * @param oldHierarchySensors Contains the sensors of the old hierarchy.
   * @param newHierarchySensors COntains the sensors of the new hierarchy.
   * @param acc The accumulator.
   */
  private static void findDeletedSensors(final Collection<Sensor> oldHierarchySensors,
      final Collection<Sensor> newHierarchySensors, final List<SensorChangedEvent> acc) {
    for (final Sensor oldSensor : oldHierarchySensors) {
      boolean deleted = true; // NOPMD redefineable variable
      for (final Sensor newSensor : newHierarchySensors) {
        if (oldSensor.getIdentifier().equals(newSensor.getIdentifier())) {
          deleted = false;
          break;
        }
      }
      if (deleted) {
        acc.add(new SensorChangedEvent(oldSensor, EventType.SENSOR_DELETED)); // NOPMD
        // PMD: instantiate object within loop is required
      }
    }
  }

  /**
   * Find all sensors that are added to or moved within the hierarchy. All sensors matching the
   * criteria are put into the accumulator as a side effect.
   *
   * @param oldHierarchySensors Contains the sensors of the old hierarchy.
   * @param newHierarchySensors COntains the sensors of the new hierarchy.
   * @param acc The accumulator.
   */
  private static void findAddedAndMovedSensors(final Collection<Sensor> oldHierarchySensors,
      final Collection<Sensor> newHierarchySensors, final List<SensorChangedEvent> acc) {
    for (final Sensor newSensor : newHierarchySensors) {
      boolean added = true; // NOPMD redefineable variable
      boolean moved = false; // NOPMD redefinable variable
      for (final Sensor oldSensor : oldHierarchySensors) {
        if (newSensor.getIdentifier().equals(oldSensor.getIdentifier())) {
          added = false;
          final boolean isParentDifferent =
              SensorHierarchyComparatorUtils.isParentDifferent(oldSensor, newSensor);
          if (isParentDifferent) {
            moved = true; // NOPMD
          }
          break;
        }
      }
      if (added) {
        acc.add(new SensorChangedEvent(newSensor, EventType.SENSOR_ADDED)); // NOPMD
        // PMD: instantiate object within loop is required
      } else if (moved) {
        acc.add(new SensorChangedEvent(newSensor, EventType.SENSOR_MOVED)); // NOPMD
        // PMD: instantiate object within loop is required
      }
    }
  }

  private static boolean isParentDifferent(final Sensor oldSensor, final Sensor newSensor) {
    final AggregatedSensor newSensorParent = newSensor.getParent().orElse(null);
    final AggregatedSensor oldSensorParent = oldSensor.getParent().orElse(null);
    return newSensorParent == null && oldSensorParent != null
        || newSensorParent != null && oldSensorParent == null
        || newSensorParent != null && oldSensorParent != null
            && !newSensorParent.getIdentifier().equals(oldSensorParent.getIdentifier());
  }

}
