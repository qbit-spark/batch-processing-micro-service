package com.qbitspark.datastorageservice;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Entity
@Table(name = "weather_data", indexes = {
        @Index(name = "idx_city", columnList = "city"),
        @Index(name = "idx_timestamp", columnList = "timestamp"),
        @Index(name = "idx_city_timestamp", columnList = "city, timestamp")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeatherDataEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private LocalDateTime timestamp;

    @Column(nullable = false, length = 50)
    private String city;

    @Column(nullable = false)
    private Double temperature;

    @Column(nullable = false)
    private Double humidity;

    @Column(nullable = false)
    private Double rainfall;

    @Column(name = "wind_speed", nullable = false)
    private Double windSpeed;

    @Column(nullable = false)
    private Double pressure;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "processed", nullable = false)
    private Boolean processed = false;

    // Multiple date formatters to handle different timestamp formats
    private static final DateTimeFormatter[] TIMESTAMP_FORMATTERS = {
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    };

    // Constructor from Kafka message (JSON parsing)
    public static WeatherDataEntity fromKafkaMessage(String jsonMessage) {
        try {
            WeatherDataEntity entity = new WeatherDataEntity();

            // Remove braces and split by commas, but be careful with commas in values
            String content = jsonMessage.trim();
            if (content.startsWith("{")) {
                content = content.substring(1);
            }
            if (content.endsWith("}")) {
                content = content.substring(0, content.length() - 1);
            }

            // Split by comma, but handle quoted values properly
            String[] pairs = splitJsonContent(content);

            for (String pair : pairs) {
                String[] keyValue = pair.split(":", 2);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim().replace("\"", "");
                    String value = keyValue[1].trim();
                    
                    // Remove quotes from string values
                    if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }

                    switch (key) {
                        case "timestamp":
                            entity.setTimestamp(parseTimestamp(value));
                            break;
                        case "city":
                            entity.setCity(value);
                            break;
                        case "temperature":
                            entity.setTemperature(parseDouble(value, "temperature"));
                            break;
                        case "humidity":
                            entity.setHumidity(parseDouble(value, "humidity"));
                            break;
                        case "rainfall":
                            entity.setRainfall(parseDouble(value, "rainfall"));
                            break;
                        case "windSpeed":
                            entity.setWindSpeed(parseDouble(value, "windSpeed"));
                            break;
                        case "pressure":
                            entity.setPressure(parseDouble(value, "pressure"));
                            break;
                    }
                }
            }

            // Set metadata
            entity.setCreatedAt(LocalDateTime.now());
            entity.setProcessed(false);

            // Validate required fields
            validateEntity(entity, jsonMessage);

            return entity;

        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing Kafka message: " + jsonMessage + " - " + e.getMessage(), e);
        }
    }

    private static String[] splitJsonContent(String content) {
        // Simple split that respects quoted strings
        return content.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    }

    private static LocalDateTime parseTimestamp(String timestampStr) {
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Timestamp cannot be null or empty");
        }

        // Try each formatter until one works
        for (DateTimeFormatter formatter : TIMESTAMP_FORMATTERS) {
            try {
                return LocalDateTime.parse(timestampStr, formatter);
            } catch (DateTimeParseException e) {
                // Continue to next formatter
            }
        }

        throw new IllegalArgumentException("Unable to parse timestamp: " + timestampStr);
    }

    private static Double parseDouble(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be null or empty");
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid " + fieldName + " value: " + value, e);
        }
    }

    private static void validateEntity(WeatherDataEntity entity, String originalMessage) {
        if (entity.getTimestamp() == null) {
            throw new IllegalArgumentException("Missing or invalid timestamp in message: " + originalMessage);
        }
        if (entity.getCity() == null || entity.getCity().trim().isEmpty()) {
            throw new IllegalArgumentException("Missing or invalid city in message: " + originalMessage);
        }
        if (entity.getTemperature() == null) {
            throw new IllegalArgumentException("Missing temperature in message: " + originalMessage);
        }
        if (entity.getHumidity() == null) {
            throw new IllegalArgumentException("Missing humidity in message: " + originalMessage);
        }
        if (entity.getRainfall() == null) {
            throw new IllegalArgumentException("Missing rainfall in message: " + originalMessage);
        }
        if (entity.getWindSpeed() == null) {
            throw new IllegalArgumentException("Missing windSpeed in message: " + originalMessage);
        }
        if (entity.getPressure() == null) {
            throw new IllegalArgumentException("Missing pressure in message: " + originalMessage);
        }
    }

    // Helper method for batch processing queries
    public boolean isFromCity(String cityName) {
        return this.city.equalsIgnoreCase(cityName);
    }

    // Helper method for time-based queries
    public boolean isInDateRange(LocalDateTime start, LocalDateTime end) {
        return timestamp.isAfter(start) && timestamp.isBefore(end);
    }
}