package com.qbitspark.dataprocessingservice;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class WeatherAnalyticsService {

    @Autowired
    private WeatherDataRepository weatherDataRepository;

    public Map<String, Object> generateQuarterlyReport() {
        log.info("🚀 Starting quarterly weather analytics report generation...");

        Map<String, Object> report = new HashMap<>();

        try {
            // Basic stats
            long totalRecords = weatherDataRepository.count();
            report.put("totalRecords", totalRecords);
            log.info("📊 Total records: {}", totalRecords);

            // Calculate global stats using Spring Data methods
            List<WeatherDataEntity> allData = weatherDataRepository.findAll();
            Map<String, Object> globalStats = calculateGlobalStats(allData);
            report.put("globalStats", globalStats);

            // City analytics - calculate manually
            Map<String, Object> cityAnalytics = calculateCityAnalytics();
            report.put("cityAnalytics", cityAnalytics);

            // Extreme weather events (no @Query needed!)
            List<WeatherDataEntity> extremeEvents = weatherDataRepository
                    .findByTemperatureGreaterThanOrRainfallGreaterThanOrWindSpeedGreaterThan(35.0, 10.0, 25.0);
            report.put("extremeWeatherCount", extremeEvents.size());
            log.info("⚡ Found {} extreme weather events", extremeEvents.size());

            // Additional analytics
            report.put("hotDaysCount", weatherDataRepository.countByTemperatureGreaterThan(30.0));
            report.put("rainyDaysCount", weatherDataRepository.countByRainfallGreaterThan(0.1));
            report.put("windyDaysCount", weatherDataRepository.countByWindSpeedGreaterThan(20.0));

            report.put("status", "SUCCESS");
            report.put("generatedAt", LocalDateTime.now());

            log.info("✅ Generated quarterly report with {} total records", totalRecords);
            return report;

        } catch (Exception e) {
            log.error("❌ Error in quarterly report generation", e);

            Map<String, Object> errorReport = new HashMap<>();
            errorReport.put("status", "ERROR");
            errorReport.put("message", e.getMessage());
            errorReport.put("timestamp", LocalDateTime.now());

            return errorReport;
        }
    }

    private Map<String, Object> calculateGlobalStats(List<WeatherDataEntity> data) {
        Map<String, Object> stats = new HashMap<>();

        if (data.isEmpty()) {
            stats.put("error", "No data available");
            return stats;
        }

        double avgTemp = data.stream().mapToDouble(WeatherDataEntity::getTemperature).average().orElse(0.0);
        double minTemp = data.stream().mapToDouble(WeatherDataEntity::getTemperature).min().orElse(0.0);
        double maxTemp = data.stream().mapToDouble(WeatherDataEntity::getTemperature).max().orElse(0.0);
        double avgHumidity = data.stream().mapToDouble(WeatherDataEntity::getHumidity).average().orElse(0.0);
        double totalRainfall = data.stream().mapToDouble(WeatherDataEntity::getRainfall).sum();

        stats.put("avgTemperature", Math.round(avgTemp * 100.0) / 100.0);
        stats.put("minTemperature", Math.round(minTemp * 100.0) / 100.0);
        stats.put("maxTemperature", Math.round(maxTemp * 100.0) / 100.0);
        stats.put("avgHumidity", Math.round(avgHumidity * 100.0) / 100.0);
        stats.put("totalRainfall", Math.round(totalRainfall * 100.0) / 100.0);

        return stats;
    }

    private Map<String, Object> calculateCityAnalytics() {
        Map<String, Object> cityAnalytics = new HashMap<>();

        // Get distinct cities using a simple approach
        List<WeatherDataEntity> sample = weatherDataRepository.findAll();
        List<String> cities = sample.stream()
                .map(WeatherDataEntity::getCity)
                .distinct()
                .toList();

        for (String city : cities) {
            List<WeatherDataEntity> cityData = weatherDataRepository.findByCity(city);

            if (!cityData.isEmpty()) {
                Map<String, Object> cityStats = new HashMap<>();
                cityStats.put("recordCount", cityData.size());
                cityStats.put("avgTemperature",
                        Math.round(cityData.stream().mapToDouble(WeatherDataEntity::getTemperature).average().orElse(0.0) * 100.0) / 100.0);
                cityStats.put("totalRainfall",
                        Math.round(cityData.stream().mapToDouble(WeatherDataEntity::getRainfall).sum() * 100.0) / 100.0);
                cityStats.put("avgHumidity",
                        Math.round(cityData.stream().mapToDouble(WeatherDataEntity::getHumidity).average().orElse(0.0) * 100.0) / 100.0);

                cityAnalytics.put(city, cityStats);
            }
        }

        return cityAnalytics;
    }

    public Map<String, Object> processUnprocessedRecords() {
        log.info("🔄 Processing unprocessed records...");

        Map<String, Object> result = new HashMap<>();
        result.put("status", "SUCCESS");
        result.put("processedRecords", 0);
        result.put("timestamp", LocalDateTime.now());

        return result;
    }
}