package titan.ccp.configuration.api.util;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import com.google.common.io.Resources; // NOCS seperate this line
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Test;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Test the comparison of sensor hierarchies.
 *
 */
public class SensorHierarchyRepositoryTest {

  /**
   * Test the comparison of sensor hierarchies.
   */
  @Test
  public void testCompareSensorHierarchies() throws IOException {
    // load initial registry
    final URL url1 = Resources.getResource("test-registry-old.json");
    final SensorRegistry oldHierarchy =
        SensorRegistry.fromJson(Resources.toString(url1, StandardCharsets.UTF_8));

    // load changed registry
    final URL url2 = Resources.getResource("test-registry-new.json");
    final SensorRegistry newHierarchy =
        SensorRegistry.fromJson(Resources.toString(url2, StandardCharsets.UTF_8));


    final List<SensorChangedEvent> result =
        SensorHierarchyComparatorUtils.compareSensorHierarchies(oldHierarchy, newHierarchy);

    assertEquals(4, result.size());

    result.forEach(event -> {
      if (event.getEventType() == EventType.SENSOR_ADDED) {
        assertEquals("new-child", event.getSensor().getIdentifier());
      } else if (event.getEventType() == EventType.SENSOR_DELETED) {
        assertEquals("old-child", event.getSensor().getIdentifier());
      } else if (event.getEventType() == EventType.SENSOR_MOVED) {
        assertThat(event.getSensor().getIdentifier(),
            anyOf(equalTo("machine-1"), equalTo("machine-2")));
      }
    });
  }

}
