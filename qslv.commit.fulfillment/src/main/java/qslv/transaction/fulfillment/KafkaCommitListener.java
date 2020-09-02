package qslv.transaction.fulfillment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.CommitReservationRequest;

@Component
public class KafkaCommitListener {
	private static final Logger log = LoggerFactory.getLogger(KafkaCommitListener.class);

	@Autowired
	private FulfillmentControllerService fulfillmentController;

	public void setFulfillmentController(FulfillmentControllerService fulfillmentController) {
		this.fulfillmentController = fulfillmentController;
	}

	@KafkaListener(topics = "#{ @configProperties.kafkaCommitRequestQueue }")
	void onCommitMessage(final ConsumerRecord<String, TraceableMessage<CommitReservationRequest>> data, Acknowledgment acknowledgment) {
		log.trace("onMessage ENTRY");

		fulfillmentController.fulfillCommit(data.value(), acknowledgment);
		log.error("========================={} {}", data.key(), data.value());

		log.trace("onMessage EXIT");
	}

}
