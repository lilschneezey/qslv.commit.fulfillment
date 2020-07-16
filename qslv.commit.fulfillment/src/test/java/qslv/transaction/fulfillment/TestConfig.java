package qslv.transaction.fulfillment;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@Configuration
public class TestConfig {

	@Autowired EmbeddedKafkaBroker embeddedKafka;

	@Bean
	public Map<String,Object> listenerConfig() throws Exception {
		HashMap<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", embeddedKafka.getBrokersAsString());
		props.put("group.id", "whatever");
		props.put("schema.registry.url", "http://localhost:8081");
	
		return props;
	}
	
	@Bean
	public Map<String,Object> producerConfig() throws Exception {
		HashMap<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", embeddedKafka.getBrokersAsString());
		props.put("group.id", "whatever");
		props.put("schema.registry.url", "http://localhost:8081");
	
		return props;
	}

}
