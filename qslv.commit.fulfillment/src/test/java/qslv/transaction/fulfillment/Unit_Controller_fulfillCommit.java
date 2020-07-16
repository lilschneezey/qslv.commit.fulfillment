package qslv.transaction.fulfillment;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.time.LocalDateTime;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.support.Acknowledgment;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transaction.response.CommitReservationResponse;


@ExtendWith(MockitoExtension.class)
class Unit_Controller_fulfillCommit {
	@Autowired
	FulfillmentControllerService fulfillmentControllerService = new FulfillmentControllerService();
	@Mock
	private ConfigProperties config;
	@Mock
	TransactionDao transactionDao;
	@Mock
	private KafkaDao kafkaDao;
	@Mock
	Acknowledgment acknowledgment;
	@Captor
	ArgumentCaptor<TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> captor;
	
	@BeforeEach
	public void setup() {
		MockitoAnnotations.initMocks(this);
		config.setAitid("234523");
		config.setKafkaTimeout(23);
		fulfillmentControllerService.setConfig(config);
		fulfillmentControllerService.setKafkaDao(kafkaDao);
		fulfillmentControllerService.setTransactionDao(transactionDao);
	}

	@Test
	void test_fulfillCommit_success() {
		//-- Setup ------------------
		TraceableMessage<CommitReservationRequest> request = setup_request();
		CommitReservationResponse commitResponse = setup_response();

		//--Prepare----------------------
		doReturn(commitResponse).when(transactionDao).commitReservation(any(), any());
		doNothing().when(kafkaDao).produceCommit(any());
		doNothing().when(acknowledgment).acknowledge();
		
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);
		
		//--Verify------------------------
		verify(kafkaDao).produceCommit(captor.capture());
		
		TraceableMessage<?> trace = captor.getValue();
		assertEquals(trace.getBusinessTaxonomyId(), request.getBusinessTaxonomyId());
		assertEquals(trace.getCorrelationId(), request.getCorrelationId());
		assertNotNull(trace.getMessageCompletionTime());
		assertEquals(trace.getMessageCreationTime(), request.getMessageCreationTime());
		assertEquals(trace.getProducerAit(), request.getProducerAit());
		
		assertSame(captor.getValue().getPayload().getRequest(), request.getPayload());
		assertSame(captor.getValue().getPayload().getResponse(), commitResponse);
	}


	@Test
	void test_fulfillCommit_restNotAvailable() {
		//-- Setup ------------------
		TraceableMessage<CommitReservationRequest> request = setup_request();

		//--Prepare----------------------
		doThrow(new TransientDataAccessResourceException("werwer")).when(transactionDao).commitReservation(any(), any());
		doNothing().when(acknowledgment).nack(anyLong());
		
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);
		
		//--Verify------------------------
	}
	
	@Test
	void test_fulfillCommit_restFailure() {
		//-- Setup ------------------
		TraceableMessage<CommitReservationRequest> request = setup_request();

		//--Prepare----------------------
		doThrow(new RuntimeException("werwer")).when(transactionDao).commitReservation(any(), any());
		doNothing().when(kafkaDao).produceCommit(any());
		doNothing().when(acknowledgment).acknowledge();
		
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);
		
		//--Verify------------------------
		verify(kafkaDao).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertNotNull(captor.getValue().getPayload().getMessage());
	}
	
	@Test
	void test_fulfillCommit_kafkaNotAvailable() {
		//-- Setup ------------------
		TraceableMessage<CommitReservationRequest> request = setup_request();
		CommitReservationResponse commitResponse = setup_response();

		//--Prepare----------------------
		doReturn(commitResponse).when(transactionDao).commitReservation(any(), any());
		doThrow(new TransientDataAccessResourceException(";asdufgha;")).when(kafkaDao).produceCommit(any());
		doNothing().when(acknowledgment).nack(anyLong());
		
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);
		
		//--Verify------------------------
	}

	@Test
	void test_fulfillCommit_kafkaFailure() {
		//-- Setup ------------------
		TraceableMessage<CommitReservationRequest> request = setup_request();
		CommitReservationResponse commitResponse = setup_response();

		//--Prepare----------------------
		doReturn(commitResponse).when(transactionDao).commitReservation(any(), any());
		doThrow(new RuntimeException("34jkdsfjlsdi")).doNothing().when(kafkaDao).produceCommit(any());
		doNothing().when(acknowledgment).acknowledge();
		
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);
		
		//--Verify------------------------
		verify(kafkaDao, times(2)).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertNotNull(captor.getValue().getPayload().getMessage());
	}
	
	@Test
	void test_fulfillCommit_twoKafkaFailures() {
		//-- Setup ------------------
		TraceableMessage<CommitReservationRequest> request = setup_request();
		CommitReservationResponse commitResponse = setup_response();

		//--Prepare----------------------
		doReturn(commitResponse).when(transactionDao).commitReservation(any(), any());
		doThrow(new RuntimeException("34jkdsfjlsdi")).when(kafkaDao).produceCommit(any());
		doNothing().when(acknowledgment).nack(anyLong());
		
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);
		
		//--Verify------------------------
	}
	
	@Test
	void test_validateInput() {
		int count = 1;
		
		//-- Prepare ------------------
		doNothing().when(kafkaDao).produceCommit(any());
		doNothing().when(acknowledgment).acknowledge();
		
		//-- Setup ------------------
		TraceableMessage<CommitReservationRequest> request = setup_request();
		CommitReservationResponse commitResponse = setup_response();
		
		request.getPayload().setRequestUuid(null);
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);		
		//--Verify------------------------
		verify(kafkaDao, times(count++)).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertTrue(captor.getValue().getPayload().getMessage().contains("Request UUID"));

		request.getPayload().setRequestUuid(UUID.randomUUID());
		request.getPayload().setReservationUuid(null);
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);		
		//--Verify------------------------
		verify(kafkaDao, times(count++)).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertTrue(captor.getValue().getPayload().getMessage().contains("Reservation UUID"));

		request.getPayload().setReservationUuid(UUID.randomUUID());
		request.getPayload().setTransactionMetaDataJson(null);
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);		
		//--Verify------------------------
		verify(kafkaDao, times(count++)).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertTrue(captor.getValue().getPayload().getMessage().contains("Meta Data"));

		request.getPayload().setTransactionMetaDataJson("{}");
		
		
		request.setProducerAit(null);
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);		
		//--Verify------------------------
		verify(kafkaDao, times(count++)).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertTrue(captor.getValue().getPayload().getMessage().contains("AIT"));

		request.setProducerAit("27384");
		request.setCorrelationId(null);
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);		
		//--Verify------------------------
		verify(kafkaDao, times(count++)).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertTrue(captor.getValue().getPayload().getMessage().contains("Correlation"));

		request.setCorrelationId("234273984728934");
		request.setBusinessTaxonomyId(null);
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);		
		//--Verify------------------------
		verify(kafkaDao, times(count++)).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertTrue(captor.getValue().getPayload().getMessage().contains("Business Taxonomy Id"));

		request.setBusinessTaxonomyId("989234230489");
		request.setMessageCreationTime(null);
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);		
		//--Verify------------------------
		verify(kafkaDao, times(count++)).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertTrue(captor.getValue().getPayload().getMessage().contains("Message Creation Time"));

		request.setMessageCreationTime(LocalDateTime.now());
		CommitReservationRequest save = request.getPayload();
		request.setPayload(null);
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);		
		//--Verify------------------------
		verify(kafkaDao, times(count++)).produceCommit(captor.capture());
		assertEquals(captor.getValue().getPayload().getStatus(), ResponseMessage.INTERNAL_ERROR);
		assertTrue(captor.getValue().getPayload().getMessage().contains("Fulfillment Message"));

		request.setPayload(save);
		
		
		
		//--Prepare----------------------
		doReturn(commitResponse).when(transactionDao).commitReservation(any(), any());
		doNothing().when(kafkaDao).produceCommit(any());
		doNothing().when(acknowledgment).acknowledge();
		
		//--Execute-----------------------
		fulfillmentControllerService.fulfillCommit(request, acknowledgment);

		//--Verify------------------------
	}
	
	CommitReservationResponse setup_response() {
		CommitReservationResponse commitResponse = new CommitReservationResponse();
		commitResponse.setResource(new TransactionResource());
		return commitResponse;
	}
	
	private TraceableMessage<CommitReservationRequest> setup_request() {
		TraceableMessage<CommitReservationRequest> request = new TraceableMessage<>();
		request.setBusinessTaxonomyId("38923748273482");
		request.setCorrelationId("2387429837428374");
		request.setMessageCreationTime(LocalDateTime.now());
		request.setProducerAit("2345");
		request.setPayload(new CommitReservationRequest());
		request.getPayload().setAccountNumber("23874923749823");
		request.getPayload().setRequestUuid(UUID.randomUUID());
		request.getPayload().setReservationUuid(UUID.randomUUID());
		request.getPayload().setTransactionMetaDataJson("{}");
		return request;
	}
}
