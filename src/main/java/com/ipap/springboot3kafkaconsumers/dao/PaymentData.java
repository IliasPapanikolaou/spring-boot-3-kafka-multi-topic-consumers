package com.ipap.springboot3kafkaconsumers.dao;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Currency;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class PaymentData {

    private String paymentReference;
    private String type;
    private BigDecimal amount;
    private Currency currency;
}
