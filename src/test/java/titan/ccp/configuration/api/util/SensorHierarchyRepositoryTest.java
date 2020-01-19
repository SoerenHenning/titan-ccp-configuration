package titan.ccp.configuration.api.util;

import static org.junit.Assert.assertEquals;
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


    final List<SensorEvent> result =
        SensorHierarchyComparatorUtils.compareSensorHierarchies(oldHierarchy, newHierarchy);

    assertEquals(result.size(), 2);

    result.forEach(event -> {
      if (event instanceof SensorAddedEvent) {
        assertEquals(event.getSensor().getIdentifier(), "new-child");
      } else if (event instanceof SensorDeletedEvent) {
        assertEquals(event.getSensor().getIdentifier(), "old-child");
      }
    });
  }

}
