package titan.ccp.configuration.events;

public interface EventPublisher {

	public void publish(final Event event, final String value);

	public void close();

}
