package com.ipap.springboot3kafkaconsumers.consumer;

import com.ipap.springboot3kafkaconsumers.SpringBoot3KafkaConsumersApplication;
import com.ipap.springboot3kafkaconsumers.dao.PaymentData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest(classes = SpringBoot3KafkaConsumersApplication.class)
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9099", "port=9099"})
// @ActiveProfiles("multiple-topics")
class PaymentDataConsumerIntegrationTest {

    private static final String CARD_PAYMENTS_TOPIC = "card-payments";
    private static final String BANK_TRANSFERS_TOPIC = "bank-transfers";

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private KafkaTemplate<String, PaymentData> kafkaProducer;

    @SpyBean
    private PaymentDataConsumer paymentDataConsumer;

    @BeforeEach
    void setUp() {
        // Wait for embedded Kafka
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, 2);
        }
    }

    @Test
    public void whenSendingMessagesOnTwoTopics_thenConsumerReceivesMessages() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        doAnswer(invocation -> {
            countDownLatch.countDown();
            return null;
        })
                .when(paymentDataConsumer)
                .handlePaymentEvents(any(), any());

        kafkaProducer.send(CARD_PAYMENTS_TOPIC, createCardPayment());
        kafkaProducer.send(BANK_TRANSFERS_TOPIC, createBankTransfer());

        assertThat(countDownLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private PaymentData createCardPayment() {
        PaymentData cardPayment = new PaymentData();
        cardPayment.setAmount(BigDecimal.valueOf(275));
        cardPayment.setPaymentReference("A184028KM0013790");
        cardPayment.setCurrency(Currency.getInstance("GBP"));
        cardPayment.setType("card");
        return cardPayment;
    }

    private PaymentData createBankTransfer() {
        PaymentData bankTransfer = new PaymentData();
        bankTransfer.setAmount(BigDecimal.valueOf(150));
        bankTransfer.setPaymentReference("19ae2-18mk73-009");
        bankTransfer.setCurrency(Currency.getInstance("EUR"));
        bankTransfer.setType("bank");
        return bankTransfer;
    }

}