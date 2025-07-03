package com.qbitspark.datastorageservice;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

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

    // Constructor from Kafka message (JSON parsing)
    public static WeatherDataEntity fromKafkaMessage(String jsonMessage) {
        try {
            // Simple JSON parsing - in production use Jackson or Gson
            String[] parts = jsonMessage
                    .replace("{", "")
                    .replace("}", "")
                    .replace("\"", "")
                    .split(",");

            WeatherDataEntity entity = new WeatherDataEntity();

            for (String part : parts) {
                String[] keyValue = part.split(":");
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();

                    switch (key) {
                        case "timestamp":
                            entity.setTimestamp(LocalDateTime.parse(value));
                            break;
                        case "city":
                            entity.setCity(value);
                            break;
                        case "temperature":
                            entity.setTemperature(Double.parseDouble(value));
                            break;
                        case "humidity":
                            entity.setHumidity(Double.parseDouble(value));
                            break;
                        case "rainfall":
                            entity.setRainfall(Double.parseDouble(value));
                            break;
                        case "windSpeed":
                            entity.setWindSpeed(Double.parseDouble(value));
                            break;
                        case "pressure":
                            entity.setPressure(Double.parseDouble(value));
                            break;
                    }
                }
            }

            entity.setCreatedAt(LocalDateTime.now());
            entity.setProcessed(false);

            return entity;

        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing Kafka message: " + jsonMessage, e);
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