package com.qbitspark.datastorageservice;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface WeatherDataRepository extends JpaRepository<WeatherDataEntity, Long> {

    List<WeatherDataEntity> findByCity(String city);

    Page<WeatherDataEntity> findByCity(String city, Pageable pageable);

    List<WeatherDataEntity> findByTimestampBetween(LocalDateTime startDate, LocalDateTime endDate);

    List<WeatherDataEntity> findByCityAndTimestampBetween(String city, LocalDateTime startDate, LocalDateTime endDate);

    List<WeatherDataEntity> findByProcessedFalseOrderByTimestampAsc();

    Page<WeatherDataEntity> findByProcessedFalseOrderByTimestampAsc(Pageable pageable);

    List<WeatherDataEntity> findByTemperatureBetween(Double minTemp, Double maxTemp);

    List<WeatherDataEntity> findByCityAndTemperatureGreaterThan(String city, Double temperature);

    List<WeatherDataEntity> findByHumidityLessThan(Double humidity);

    List<WeatherDataEntity> findByRainfallGreaterThan(Double rainfall);

    long countByCity(String city);

    long countByProcessedFalse();

    long countByTimestampBetween(LocalDateTime startDate, LocalDateTime endDate);

    List<WeatherDataEntity> findByTimestampAfter(LocalDateTime since);

    List<WeatherDataEntity> findByCityIn(List<String> cities);

    List<WeatherDataEntity> findByTemperatureGreaterThanOrderByTemperatureDesc(Double threshold);

    List<WeatherDataEntity> findByCityAndProcessed(String city, Boolean processed);

    List<WeatherDataEntity> findTop10ByOrderByTimestampDesc();

    List<WeatherDataEntity> findByCityOrderByTimestampDesc(String city);

    void deleteByProcessedTrueAndCreatedAtBefore(LocalDateTime cutoffDate);

    boolean existsByCity(String city);

    boolean existsByTimestampBetween(LocalDateTime start, LocalDateTime end);

}