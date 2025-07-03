package com.qbitspark.datastorageservice;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
@RequiredArgsConstructor
public class WeatherDataConsumer {


    private final WeatherDataRepository weatherDataRepository;

    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    @KafkaListener(topics = "weather-data", groupId = "weather-storage-group")
    @Transactional
    public void consumeWeatherData(@Payload String message,
                                   @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) long offset) {

        try {
            log.debug("Received weather data - Key: {}, Partition: {}, Offset: {}", key, partition, offset);


            WeatherDataEntity weatherEntity = WeatherDataEntity.fromKafkaMessage(message);


            WeatherDataEntity savedEntity = weatherDataRepository.save(weatherEntity);

            // Update counters
            long count = processedCount.incrementAndGet();


            if (count % 1000 == 0) {
                log.info("✅ Stored {} weather records in database. Latest: {} - {}",
                        count, savedEntity.getCity(), savedEntity.getTimestamp());
            }

            // Log city-specific progress every 10000 records
            if (count % 10000 == 0) {
                long totalRecords = weatherDataRepository.count();
                long unprocessedRecords = weatherDataRepository.countByProcessedFalse();

                log.info("📊 Database Status - Total: {}, Unprocessed: {}, Error Count: {}",
                        totalRecords, unprocessedRecords, errorCount.get());
            }

        } catch (Exception e) {
            long errors = errorCount.incrementAndGet();
            log.error("❌ Error processing weather data message [Error #{}]: {}", errors, message, e);

            // Continue processing other messages even if one fails
            // In production, you might want to send to a dead letter queue
        }
    }

    @KafkaListener(topics = "weather-data", groupId = "weather-storage-monitoring-group")
    public void monitorWeatherData(@Payload String message,
                                   @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        // This is a separate consumer group for monitoring only
        // Doesn't interfere with the main storage consumer

        try {
            // Extract city from a message for monitoring
            String city = extractCityFromMessage(message);

            // Log city-specific activity (useful for debugging)
            log.debug("🌦️  Weather data received for city: {}", city);

        } catch (Exception e) {
            log.warn("⚠️  Could not extract city from message for monitoring: {}", message);
        }
    }

    private String extractCityFromMessage(String jsonMessage) {
        try {
            // Simple extraction - look for "city":"value"
            String cityPattern = "\"city\":\"";
            int startIndex = jsonMessage.indexOf(cityPattern);
            if (startIndex != -1) {
                startIndex += cityPattern.length();
                int endIndex = jsonMessage.indexOf("\"", startIndex);
                if (endIndex != -1) {
                    return jsonMessage.substring(startIndex, endIndex);
                }
            }
            return "Unknown";
        } catch (Exception e) {
            return "Unknown";
        }
    }

    // Method to get consumer statistics
    public ConsumerStats getConsumerStats() {
        long totalRecords = weatherDataRepository.count();
        long unprocessedRecords = weatherDataRepository.countByProcessedFalse();

        return new ConsumerStats(
                processedCount.get(),
                errorCount.get(),
                totalRecords,
                unprocessedRecords
        );
    }

    // Reset counters (useful for testing)
    public void resetCounters() {
        processedCount.set(0);
        errorCount.set(0);
        log.info("🔄 Consumer counters reset");
    }

    // Inner class for statistics
    public static class ConsumerStats {
        public final long messagesProcessed;
        public final long errors;
        public final long totalDatabaseRecords;
        public final long unprocessedRecords;

        public ConsumerStats(long messagesProcessed, long errors, long totalDatabaseRecords, long unprocessedRecords) {
            this.messagesProcessed = messagesProcessed;
            this.errors = errors;
            this.totalDatabaseRecords = totalDatabaseRecords;
            this.unprocessedRecords = unprocessedRecords;
        }

        @Override
        public String toString() {
            return String.format("ConsumerStats{processed=%d, errors=%d, totalDB=%d, unprocessed=%d}",
                    messagesProcessed, errors, totalDatabaseRecords, unprocessedRecords);
        }
    }
}