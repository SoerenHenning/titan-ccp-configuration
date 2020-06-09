package titan.ccp.configuration.api.util.jsondeserialization;

import java.util.List;

/**
 * Class for automatic GSON deserialization for colliding sensor identifiers.
 */
@SuppressWarnings("PMD")
public class CollisionsType {
  private final List<String> collisions;

  public CollisionsType(final List<String> collisions) {
    this.collisions = collisions;
  }
}
