package com.qbitspark.dataingestionservice;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletableFuture;
import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/api/weather")
@Slf4j
public class WeatherDataController {

    @Autowired
    private WeatherDataProducer weatherDataProducer;

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Data Ingestion Service");
        response.put("timestamp", java.time.LocalDateTime.now().toString());
        return ResponseEntity.ok(response);
    }

    @PostMapping("/ingest")
    public ResponseEntity<Map<String, String>> ingestWeatherData(@RequestParam String csvFilePath) {
        log.info("Received request to ingest weather data from: {}", csvFilePath);

        Map<String, String> response = new HashMap<>();

        try {
            // Run ingestion asynchronously to avoid timeout
            CompletableFuture.runAsync(() -> {
                weatherDataProducer.ingestWeatherDataFromCsv(csvFilePath);
            });

            response.put("status", "STARTED");
            response.put("message", "Weather data ingestion started successfully");
            response.put("csvFile", csvFilePath);
            response.put("timestamp", java.time.LocalDateTime.now().toString());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error starting weather data ingestion", e);

            response.put("status", "ERROR");
            response.put("message", "Failed to start ingestion: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now().toString());

            return ResponseEntity.status(500).body(response);
        }
    }

    @PostMapping("/test")
    public ResponseEntity<Map<String, String>> sendTestData() {
        log.info("Sending test weather data to Kafka");

        Map<String, String> response = new HashMap<>();

        try {
            weatherDataProducer.sendTestData();

            response.put("status", "SUCCESS");
            response.put("message", "Test weather data sent to Kafka successfully");
            response.put("timestamp", java.time.LocalDateTime.now().toString());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error sending test data", e);

            response.put("status", "ERROR");
            response.put("message", "Failed to send test data: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now().toString());

            return ResponseEntity.status(500).body(response);
        }
    }

    @PostMapping("/ingest/local")
    public ResponseEntity<Map<String, String>> ingestLocalWeatherData() {
        // Default path for our Tanzania weather data
        String defaultCsvPath = "tanzania_weather_data.csv";

        log.info("Ingesting local Tanzania weather data from: {}", defaultCsvPath);

        Map<String, String> response = new HashMap<>();

        try {
            // Run ingestion asynchronously
            CompletableFuture.runAsync(() -> {
                weatherDataProducer.ingestWeatherDataFromCsv(defaultCsvPath);
            });

            response.put("status", "STARTED");
            response.put("message", "Tanzania weather data ingestion started");
            response.put("csvFile", defaultCsvPath);
            response.put("expectedRecords", "2,300,000+");
            response.put("timestamp", java.time.LocalDateTime.now().toString());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error starting local weather data ingestion", e);

            response.put("status", "ERROR");
            response.put("message", "Failed to start ingestion: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now().toString());

            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, String>> getIngestionStatus() {
        Map<String, String> response = new HashMap<>();
        response.put("service", "Weather Data Ingestion");
        response.put("description", "Kafka producer for Tanzania weather data");
        response.put("kafkaTopic", "weather-data");
        response.put("supportedFormats", "CSV");
        response.put("timestamp", java.time.LocalDateTime.now().toString());

        return ResponseEntity.ok(response);
    }
}