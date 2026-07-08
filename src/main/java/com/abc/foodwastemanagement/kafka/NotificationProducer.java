package com.abc.foodwastemanagement.kafka;

import com.abc.foodwastemanagement.enums.KafkaNotificationType;
import com.abc.foodwastemanagement.notification.NotificationService;
import com.abc.foodwastemanagement.notification.event.NotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * NotificationProducer
 * Modified to bypass Kafka and invoke NotificationService directly.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class NotificationProducer {

    private final NotificationService notificationService;

    public void publish(
            String userId,
            KafkaNotificationType eventType,
            String title,
            String message
    ) {

        NotificationEvent event = new NotificationEvent(
                UUID.randomUUID().toString(),
                eventType,
                userId,
                title,
                message,
                Instant.now()
        );

        try {
            // Bypass Kafka and call the notification service directly (in-memory execution)
            notificationService.createFromEvent(event);

            log.info(
                "Local notification processed directly -> userId={}, type={}",
                userId,
                eventType
            );

        } catch (Exception ex) {

            log.warn(
                "Failed to process notification. eventId={}, userId={}, type={}",
                event.getEventId(),
                userId,
                eventType,
                ex
            );
        }
    }
}
