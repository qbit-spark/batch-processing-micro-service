package com.qbitspark.datadeliveryservice;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface WeatherDataRepository extends JpaRepository<WeatherDataEntity, Long> {

    // Basic city queries
    List<WeatherDataEntity> findByCity(String city);
    Page<WeatherDataEntity> findByCity(String city, Pageable pageable);

    // Temperature filtering
    Page<WeatherDataEntity> findByTemperatureBetween(Double minTemp, Double maxTemp, Pageable pageable);
    Page<WeatherDataEntity> findByTemperatureGreaterThanEqual(Double minTemp, Pageable pageable);
    Page<WeatherDataEntity> findByTemperatureLessThanEqual(Double maxTemp, Pageable pageable);

    // Date range queries
    List<WeatherDataEntity> findByTimestampBetween(LocalDateTime startDate, LocalDateTime endDate);
    Page<WeatherDataEntity> findByTimestampBetween(LocalDateTime startDate, LocalDateTime endDate, Pageable pageable);

    // Combined city and date queries
    List<WeatherDataEntity> findByCityAndTimestampBetween(String city, LocalDateTime startDate, LocalDateTime endDate);
    Page<WeatherDataEntity> findByCityAndTimestampBetween(String city, LocalDateTime startDate, LocalDateTime endDate, Pageable pageable);

    // Latest data queries
    List<WeatherDataEntity> findTop10ByOrderByTimestampDesc();
    List<WeatherDataEntity> findTop50ByOrderByTimestampDesc();

    // City-specific latest data
    List<WeatherDataEntity> findByCityOrderByTimestampDesc(String city);

    // Statistics queries
    long countByCity(String city);

    // Temperature-based queries for statistics
    long countByTemperatureGreaterThan(Double temperature);
    long countByTemperatureLessThan(Double temperature);

    // Rainfall queries
    long countByRainfallGreaterThan(Double rainfall);
    List<WeatherDataEntity> findByRainfallGreaterThan(Double rainfall);

    // Humidity queries
    List<WeatherDataEntity> findByHumidityBetween(Double minHumidity, Double maxHumidity);

    // Wind speed queries
    List<WeatherDataEntity> findByWindSpeedGreaterThan(Double windSpeed);

    // Complex filtering - you can extend this for more advanced searches
    @Query("SELECT w FROM WeatherDataEntity w WHERE " +
            "(:city IS NULL OR w.city = :city) AND " +
            "(:startDate IS NULL OR w.timestamp >= :startDate) AND " +
            "(:endDate IS NULL OR w.timestamp <= :endDate) AND " +
            "(:minTemp IS NULL OR w.temperature >= :minTemp) AND " +
            "(:maxTemp IS NULL OR w.temperature <= :maxTemp)")
    Page<WeatherDataEntity> findWithFilters(
            @Param("city") String city,
            @Param("startDate") LocalDateTime startDate,
            @Param("endDate") LocalDateTime endDate,
            @Param("minTemp") Double minTemp,
            @Param("maxTemp") Double maxTemp,
            Pageable pageable);

    // Get distinct cities
    @Query("SELECT DISTINCT w.city FROM WeatherDataEntity w ORDER BY w.city")
    List<String> findDistinctCities();

    // Get latest records for each city
    @Query("SELECT w FROM WeatherDataEntity w WHERE w.timestamp = " +
            "(SELECT MAX(w2.timestamp) FROM WeatherDataEntity w2 WHERE w2.city = w.city)")
    List<WeatherDataEntity> findLatestForEachCity();

    // Get weather summary by city
    @Query("SELECT w.city, COUNT(w), AVG(w.temperature), AVG(w.humidity), SUM(w.rainfall) " +
            "FROM WeatherDataEntity w GROUP BY w.city")
    List<Object[]> getWeatherSummaryByCity();

    // Check if data exists for date range
    boolean existsByTimestampBetween(LocalDateTime startDate, LocalDateTime endDate);

    // Find extreme weather conditions
    List<WeatherDataEntity> findByTemperatureGreaterThanOrRainfallGreaterThanOrWindSpeedGreaterThan(
            Double tempThreshold, Double rainfallThreshold, Double windThreshold);
}