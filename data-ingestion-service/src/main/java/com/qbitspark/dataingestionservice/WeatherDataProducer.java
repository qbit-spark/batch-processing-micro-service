package com.qbitspark.dataingestionservice;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
@Service
@Slf4j
public class WeatherDataProducer {

    private static final String TOPIC_NAME = "weather-data";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void ingestWeatherDataFromCsv(String csvFilePath) {
        log.info("Starting weather data ingestion from: {}", csvFilePath);

        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            int recordCount = 0;
            int batchCount = 0;

            // Skip header line
            reader.readLine();

            while ((line = reader.readLine()) != null) {
                try {
                    // Parse CSV line to WeatherRecord
                    WeatherRecord weatherRecord = WeatherRecord.fromCsvLine(line);

                    // Send to Kafka (async)
                    sendWeatherRecord(weatherRecord);

                    recordCount++;

                    // Log progress every 10,000 records
                    if (recordCount % 10000 == 0) {
                        log.info("Processed {} weather records...", recordCount);
                    }

                } catch (Exception e) {
                    log.error("Error processing line: {}", line, e);
                }
            }

            log.info("✅ Weather data ingestion completed! Total records processed: {}", recordCount);

        } catch (IOException e) {
            log.error("Error reading CSV file: {}", csvFilePath, e);
            throw new RuntimeException("Failed to read weather data file", e);
        }
    }

    private void sendWeatherRecord(WeatherRecord weatherRecord) {
        try {
            // Use city as the key for partitioning
            String key = weatherRecord.getCity();
            String value = weatherRecord.toString(); // JSON format

            // Send to Kafka topic
            CompletableFuture<org.springframework.kafka.support.SendResult<String, String>> future =
                    kafkaTemplate.send(TOPIC_NAME, key, value);

            // Optional: Handle success/failure (for production)
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send weather record for city: {}", weatherRecord.getCity(), ex);
                }
            });

        } catch (Exception e) {
            log.error("Error sending weather record to Kafka: {}", weatherRecord, e);
        }
    }

    // Method to send single weather record (for testing)
    public void sendSingleRecord(WeatherRecord weatherRecord) {
        sendWeatherRecord(weatherRecord);
        log.info("Sent single weather record: {}", weatherRecord.getCity());
    }

    // Method to send test data
    public void sendTestData() {
        log.info("Sending test weather data...");

        WeatherRecord testRecord = new WeatherRecord(
                java.time.LocalDateTime.now(),
                "Mbeya",
                18.5,
                75.0,
                0.2,
                12.5,
                1015.3
        );

        sendSingleRecord(testRecord);
    }
}