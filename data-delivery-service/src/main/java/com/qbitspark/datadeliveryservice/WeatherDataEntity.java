package com.qbitspark.datadeliveryservice;

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

    // Helper methods for data delivery formatting

    public String getFormattedTimestamp() {
        return timestamp.toString();
    }

    public String getTemperatureCategory() {
        if (temperature < 10) return "VERY_COLD";
        if (temperature < 15) return "COLD";
        if (temperature < 25) return "MILD";
        if (temperature < 30) return "WARM";
        if (temperature < 35) return "HOT";
        return "VERY_HOT";
    }

    public String getRainfallCategory() {
        if (rainfall == 0) return "NO_RAIN";
        if (rainfall < 1) return "LIGHT_RAIN";
        if (rainfall < 5) return "MODERATE_RAIN";
        if (rainfall < 10) return "HEAVY_RAIN";
        return "VERY_HEAVY_RAIN";
    }

    public String getHumidityCategory() {
        if (humidity < 30) return "VERY_DRY";
        if (humidity < 50) return "DRY";
        if (humidity < 70) return "COMFORTABLE";
        if (humidity < 80) return "HUMID";
        return "VERY_HUMID";
    }

    public boolean isHighTemperature() {
        return temperature > 30.0;
    }

    public boolean isRainyDay() {
        return rainfall > 0.1;
    }

    public boolean isHighHumidity() {
        return humidity > 80.0;
    }

    public boolean isWindy() {
        return windSpeed > 20.0;
    }

    // For JSON serialization in API responses
    @Override
    public String toString() {
        return String.format("WeatherData{city='%s', timestamp=%s, temp=%.1f°C, humidity=%.1f%%, rainfall=%.2fmm}",
                city, timestamp, temperature, humidity, rainfall);
    }
}