package titan.ccp.configuration;

public interface EventPublisher {

	public void publish(final Event event, final String value);

	public void close();

}
