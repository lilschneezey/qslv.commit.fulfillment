package qslv.transaction.fulfillment;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import com.fasterxml.jackson.databind.JavaType;

import qslv.common.TimedResponse;
import qslv.common.kafka.JacksonAvroDeserializer;
import qslv.common.kafka.JacksonAvroSerializer;
import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transaction.response.CommitReservationResponse;

@SpringBootTest
@Import(value = { TestConfig.class })
@DirtiesContext
@EmbeddedKafka(partitions = 1,topics = { "commit.request.queue", "commit.reply.queue" })
@ActiveProfiles("test")
public class Unit_EmbeddedKakfa {
	private static String request_topic = "commit.request.queue";
	private static String reply_topic = "commit.reply.queue";
	@Autowired EmbeddedKafkaBroker embeddedKafka;
	@Autowired KafkaCommitListener kafkaCommitListener;
	@Autowired KafkaListenerConfig kafkaListenerConfig;	
	@Autowired TransactionDao transactionDao;
	@Autowired ConfigProperties configProperties;
	
	@Mock
	RestTemplateProxy restTemplateProxy;
	
	@BeforeEach
	public void init() {
		transactionDao.setRestTemplateProxy(restTemplateProxy);	
		configProperties.setKafkaCommitRequestQueue(request_topic);
		configProperties.setKafkaCommitReplyQueue(reply_topic);
	}
	
	//----------------
	// Kafka Reconfigure
	//----------------
	private Map<String,Object> embeddedConfig() {
		HashMap<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", embeddedKafka.getBrokersAsString());
		props.put("group.id", "whatever");
		props.put("schema.registry.url", "http://localhost:8081");
		return props;
	}
	
	@Test
	void test_commitReservation_success() {

		// -Setup input output----------------
		Producer<String, TraceableMessage<CommitReservationRequest>> producer = buildProducer();
		Consumer<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> consumer = buildConsumer();
		ResponseEntity<TimedResponse<CommitReservationResponse>> response = setup_responseEntity();
		
		// -Prepare----------------------------
		doReturn(response).when(restTemplateProxy).exchange(anyString(), eq(HttpMethod.POST), 
				ArgumentMatchers.<HttpEntity<TraceableMessage<CommitReservationRequest>>>any(), 
				ArgumentMatchers.<ParameterizedTypeReference<TimedResponse<CommitReservationResponse>>>any());
		//doNothing().when(fulfillmentController).fulfillCommit(any(), any());
		
		TraceableMessage<CommitReservationRequest> message = setup_traceable();
		
		// -Execute----------------
		producer.send(new ProducerRecord<>(request_topic, message.getPayload().getAccountNumber(), message));
		producer.flush();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, reply_topic);
		ConsumerRecord<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> record 
			= KafkaTestUtils.getSingleRecord(consumer, reply_topic, 2000L);

		// -Verify----------------
		verify(restTemplateProxy).exchange(anyString(), eq(HttpMethod.POST), 
				ArgumentMatchers.<HttpEntity<TraceableMessage<CommitReservationRequest>>>any(), 
				ArgumentMatchers.<ParameterizedTypeReference<TimedResponse<CommitReservationResponse>>>any());
		assertNotNull(record);
		assertEquals(message.getPayload().getAccountNumber(), record.key());
		assertEquals(message.getBusinessTaxonomyId(), record.value().getBusinessTaxonomyId());
		assertEquals(message.getPayload().getAccountNumber(), record.value().getPayload().getRequest().getAccountNumber());
	}

	//------------------------------------------------------
	// Mock producer and consumer to test end-to-end flow
	//------------------------------------------------------
	private Producer<String, TraceableMessage<CommitReservationRequest>> buildProducer() {
		Map<String, Object> configs = embeddedConfig();
		
    	JacksonAvroSerializer<TraceableMessage<CommitReservationRequest>> jas = new JacksonAvroSerializer<>();
		JavaType type = jas.getTypeFactory().constructParametricType(TraceableMessage.class, CommitReservationRequest.class);
    	jas.configure(configs, false, type);
		
		return new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), jas ).createProducer();
	}
	private Consumer<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> buildConsumer() {
		Map<String, Object> configs = embeddedConfig();
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		configs.put("group.id", "somevalue");
		
    	JacksonAvroDeserializer<TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> jad = new JacksonAvroDeserializer<>();
    	jad.configure(configs);

		return new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), jad).createConsumer();
	}
	
	TraceableMessage<CommitReservationRequest> setup_traceable() {
		TraceableMessage<CommitReservationRequest> message = new TraceableMessage<>();
		message.setPayload(setup_request());
		message.setBusinessTaxonomyId("234234234234");
		message.setCorrelationId("328942834234j23k4");
		message.setMessageCreationTime(LocalDateTime.now());
		message.setProducerAit("27834");
		return message;
	}
	CommitReservationRequest setup_request() {
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber("12345634579");
		request.setRequestUuid(UUID.randomUUID());
		request.setReservationUuid(UUID.randomUUID());
		request.setTransactionMetaDataJson("{}");
		return request;
	}
	
	ResponseEntity<TimedResponse<CommitReservationResponse>> setup_responseEntity() {
		return new ResponseEntity<TimedResponse<CommitReservationResponse>>(new TimedResponse<>(123456L, setup_response()), HttpStatus.CREATED);
	}
	ResponseEntity<TimedResponse<CommitReservationResponse>> setup_failedResponseEntity() {
		return new ResponseEntity<TimedResponse<CommitReservationResponse>>(new TimedResponse<>(234567L, setup_response()), HttpStatus.INTERNAL_SERVER_ERROR);
	}
	CommitReservationResponse setup_response() {
		CommitReservationResponse resourceResponse = new CommitReservationResponse(CommitReservationResponse.SUCCESS, new TransactionResource());
		resourceResponse.getResource().setAccountNumber("12345679");
		resourceResponse.getResource().setDebitCardNumber("7823478239467");
		return resourceResponse;
	}
}
