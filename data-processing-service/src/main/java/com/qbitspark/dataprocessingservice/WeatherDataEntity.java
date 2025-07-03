package com.qbitspark.dataprocessingservice;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "weather_data")
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

    // Analytics helper methods

    public boolean isHighTemperature() {
        return temperature > 30.0;
    }

    public boolean isLowTemperature() {
        return temperature < 15.0;
    }

    public boolean isRainyDay() {
        return rainfall > 0.1; // More than 0.1mm
    }

    public boolean isHeavyRain() {
        return rainfall > 10.0; // More than 10mm
    }

    public boolean isHighHumidity() {
        return humidity > 80.0;
    }

    public boolean isWindy() {
        return windSpeed > 20.0; // More than 20 km/h
    }

    public boolean isExtremeWeather() {
        return isHighTemperature() || isHeavyRain() || isWindy();
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

    public int getMonth() {
        return timestamp.getMonthValue();
    }

    public int getYear() {
        return timestamp.getYear();
    }

    public int getHour() {
        return timestamp.getHour();
    }

    public String getSeason() {
        int month = getMonth();
        if (month >= 12 || month <= 2) return "SUMMER"; // Dec-Feb (Tanzania summer)
        if (month >= 3 && month <= 5) return "AUTUMN";  // Mar-May (rainy season)
        if (month >= 6 && month <= 8) return "WINTER";  // Jun-Aug (dry season)
        return "SPRING"; // Sep-Nov
    }

    public boolean isInDateRange(LocalDateTime start, LocalDateTime end) {
        return timestamp.isAfter(start) && timestamp.isBefore(end);
    }

    public boolean isFromCity(String cityName) {
        return this.city.equalsIgnoreCase(cityName);
    }

    // For analytics aggregations
    public Double getTemperatureRounded() {
        return Math.round(temperature * 10.0) / 10.0;
    }

    public Double getRainfallRounded() {
        return Math.round(rainfall * 100.0) / 100.0;
    }

    public Double getHumidityRounded() {
        return Math.round(humidity * 10.0) / 10.0;
    }

    @Override
    public String toString() {
        return String.format("WeatherData{city='%s', timestamp=%s, temp=%.1f°C, humidity=%.1f%%, rainfall=%.2fmm}",
                city, timestamp, temperature, humidity, rainfall);
    }
}