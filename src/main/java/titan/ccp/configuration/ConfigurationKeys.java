package titan.ccp.configuration;

/**
 * Keys to access configuration parameters.
 */
public final class ConfigurationKeys { // NOPMD

  public static final String FAILSAFE_DELAYINMILLIS = "db.delayInMillis";

  public static final String FAILSAFE_MAXRETRIES = "db.maxRetries";

  public static final String EVENT_PUBLISHING = "event.publishing";

  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

  public static final String KAFKA_TOPIC = "kafka.topic";

  public static final String REDIS_HOST = "redis.host";

  public static final String REDIS_PORT = "redis.port";

  public static final String DEMO = "demo";

  public static final String WEBSERVER_PORT = "webserver.port";

  public static final String CORS = "webserver.cors";

  private ConfigurationKeys() {}
}
