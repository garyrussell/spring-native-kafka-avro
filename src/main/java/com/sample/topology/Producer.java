package com.sample.topology;

import com.sample.avro.Address;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Configuration
@Slf4j
public class Producer {
    @Value("${topic.name}")
    private String TOPIC;

    private final KafkaTemplate<String, Address> kafkaTemplate;

    @Autowired
    public Producer(KafkaTemplate<String, Address> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(Address address) {
        Future<SendResult<String, Address>> future = this.kafkaTemplate.send(this.TOPIC, address.getAddressId(), address);
        try {
            SendResult<String, Address> result = future.get(10, TimeUnit.SECONDS);
            log.info(result.getRecordMetadata().toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        log.info(String.format("Produced address -> %s", address));
    }

}
