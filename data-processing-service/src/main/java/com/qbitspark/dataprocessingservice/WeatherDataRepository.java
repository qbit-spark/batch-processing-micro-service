package com.qbitspark.dataprocessingservice;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface WeatherDataRepository extends JpaRepository<WeatherDataEntity, Long> {

    // Basic queries - no @Query needed!
    List<WeatherDataEntity> findByCity(String city);
    List<WeatherDataEntity> findByTimestampBetween(LocalDateTime start, LocalDateTime end);

    // Counting
    long countByCity(String city);
    long countByTemperatureGreaterThan(Double temperature);
    long countByRainfallGreaterThan(Double rainfall);
    long countByWindSpeedGreaterThan(Double windSpeed);

    // Temperature queries
    List<WeatherDataEntity> findByTemperatureGreaterThan(Double temperature);
    List<WeatherDataEntity> findByTemperatureLessThan(Double temperature);
    List<WeatherDataEntity> findByTemperatureBetween(Double min, Double max);

    // Rainfall queries
    List<WeatherDataEntity> findByRainfallGreaterThan(Double rainfall);
    List<WeatherDataEntity> findByRainfallBetween(Double min, Double max);

    // Extreme weather (combining conditions)
    List<WeatherDataEntity> findByTemperatureGreaterThanOrRainfallGreaterThanOrWindSpeedGreaterThan(
            Double tempThreshold, Double rainfallThreshold, Double windThreshold);

    // City-specific queries
    List<WeatherDataEntity> findByCityAndTemperatureGreaterThan(String city, Double temperature);
    List<WeatherDataEntity> findByCityAndRainfallGreaterThan(String city, Double rainfall);

    // Date-based queries
    List<WeatherDataEntity> findByTimestampAfter(LocalDateTime date);
    List<WeatherDataEntity> findByTimestampBefore(LocalDateTime date);

    // Ordering
    List<WeatherDataEntity> findTop10ByOrderByTemperatureDesc();
    List<WeatherDataEntity> findTop10ByOrderByRainfallDesc();
    List<WeatherDataEntity> findByCityOrderByTimestampDesc(String city);
}
