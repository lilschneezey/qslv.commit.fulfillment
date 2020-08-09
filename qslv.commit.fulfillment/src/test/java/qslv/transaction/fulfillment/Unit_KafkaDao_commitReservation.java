package qslv.transaction.fulfillment;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.response.CommitReservationResponse;

@ExtendWith(MockitoExtension.class)
public class Unit_KafkaDao_commitReservation {
	KafkaProducerDao kafkaDao = new KafkaProducerDao();
	ConfigProperties config = new ConfigProperties();
	
	@Mock
	KafkaTemplate<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> commitKafkaTemplate;
	@Mock
	ListenableFuture<SendResult<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>>> future;
	
	{
		config.setKafkaCommitReplyQueue("CommitURL");
		kafkaDao.setConfig(config);
	}
	
	@BeforeEach
	public void setup() {
		kafkaDao.setCommitKafkaTemplate(commitKafkaTemplate);
	}
	
	TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>> setup_message() {
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>> message = 
				new TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>> ();
		
		message.setPayload(new ResponseMessage<CommitReservationRequest,CommitReservationResponse>());
		message.getPayload().setRequest(new CommitReservationRequest());
		message.getPayload().getRequest().setAccountNumber("2839420384902");
		
		return message;
	}
	
	@Test
	public void test_produceCommit_success() throws InterruptedException, ExecutionException {
		
		//-Setup---------------
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>> setup_message = setup_message();
		
		//-Prepare---------------
		ProducerRecord<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> producerRecord 
			= new ProducerRecord<>("mockTopicName", setup_message);
		
		SendResult<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> sendResult 
			= new SendResult<>(producerRecord, new RecordMetadata(new TopicPartition("mockTopic", 1), 1, 1, 1, 1L, 1, 1));
		
		doReturn(sendResult).when(future).get();
		doReturn(future).when(commitKafkaTemplate).send(anyString(), anyString(), any());
	
		//-Execute----------------------------		
		kafkaDao.produceCommit(setup_message);
		
		//-Verify----------------------------		
		verify(future).get();
		ArgumentCaptor<String> arg = ArgumentCaptor.forClass(String.class);
		verify(commitKafkaTemplate).send(anyString(), arg.capture(), any());
		assertEquals( arg.getValue(), setup_message.getPayload().getRequest().getAccountNumber());
	}
	
	@Test
	public void test_produceTransferMessage_throwsTransient() throws InterruptedException, ExecutionException, TimeoutException {
		
		//-Prepare---------------
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>> setup_message = setup_message();

		//-Prepare---------------
		doThrow(new InterruptedException()).when(future).get();
		doReturn(future).when(commitKafkaTemplate).send(anyString(), anyString(), any());
		
		//--Execute--------------	
		assertThrows(TransientDataAccessResourceException.class, () -> {
			kafkaDao.produceCommit(setup_message);
		});
	}

}
