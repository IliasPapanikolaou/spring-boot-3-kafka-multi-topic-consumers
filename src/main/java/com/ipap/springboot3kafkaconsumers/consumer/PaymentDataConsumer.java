package com.ipap.springboot3kafkaconsumers.consumer;

import com.ipap.springboot3kafkaconsumers.dao.PaymentData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class PaymentDataConsumer {

    private final Logger log = LoggerFactory.getLogger(PaymentDataConsumer.class);

    @KafkaListener(topics = {"card-payments", "bank-transfers"}, groupId = "payments")
    public void handlePaymentEvents(PaymentData paymentData, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Event on topic={}, payment={}", topic, paymentData);
    }
}
