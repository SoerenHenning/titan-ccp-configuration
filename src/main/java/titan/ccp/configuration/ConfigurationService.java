package titan.ccp.configuration;

import org.apache.commons.configuration2.Configuration;

import redis.clients.jedis.Jedis;
import spark.Spark;
import titan.ccp.common.configuration.Configurations;

public class ConfigurationService {

	private final Configuration configuration = Configurations.create();
	private final Jedis jedis;

	public ConfigurationService() {
		this.jedis = new Jedis(this.configuration.getString("redis.host"), this.configuration.getInt("redis.port"));
	}

	public void start() {
		Spark.port(this.configuration.getInt("webserver.port"));

		if (this.configuration.getBoolean("webserver.cors")) {
			Spark.options("/*", (request, response) -> {

				final String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
				if (accessControlRequestHeaders != null) {
					response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
				}

				final String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
				if (accessControlRequestMethod != null) {
					response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
				}

				return "OK";
			});

			Spark.before((request, response) -> {
				response.header("Access-Control-Allow-Origin", "*");
			});
		}

		Spark.get("/sensor-registry/", (request, response) -> {
			final String redisResponse = this.jedis.get("sensor_registry");
			if (redisResponse == null) {
				response.status(500);
				return "Internal Server Error";
			} else {
				return redisResponse;
			}
		});

		Spark.put("/sensor-registry/", (request, response) -> {
			// TODO validation
			final String redisResponse = this.jedis.set("sensor_registry", request.body());
			if ("OK".equals(redisResponse)) {
				response.status(204);
				return "";
			} else {
				response.status(500);
				return "Internal Server Error";
			}
		});

		Spark.after((request, response) -> {
			response.type("application/json");
		});
	}

	public static void main(final String[] args) {
		new ConfigurationService().start();
	}

}
