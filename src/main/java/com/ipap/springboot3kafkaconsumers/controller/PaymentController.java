package com.ipap.springboot3kafkaconsumers.controller;

import com.ipap.springboot3kafkaconsumers.dao.PaymentData;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Currency;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class PaymentController {

    private static final String CARD_PAYMENTS_TOPIC = "card-payments";
    private static final String BANK_TRANSFERS_TOPIC = "bank-transfers";

    private final KafkaTemplate<String, PaymentData> kafkaProducer;

    @GetMapping("/produce-message")
    public ResponseEntity<String> publishToTopicsTrigger() {
        kafkaProducer.send(CARD_PAYMENTS_TOPIC, createCardPayment());
        kafkaProducer.send(BANK_TRANSFERS_TOPIC, createBankTransfer());
        return ResponseEntity.ok("Messages enqueued");
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
