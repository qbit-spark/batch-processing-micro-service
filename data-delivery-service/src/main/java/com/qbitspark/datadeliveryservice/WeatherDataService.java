package com.qbitspark.datadeliveryservice;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class WeatherDataService {

    @Autowired
    private WeatherDataRepository weatherDataRepository;

    public long getTotalRecords() {
        return weatherDataRepository.count();
    }

    public List<String> getAllCities() {
        log.info("🏙️  Fetching all distinct cities");

        // Get a sample of records and extract unique cities
        List<WeatherDataEntity> allRecords = weatherDataRepository.findAll();
        List<String> cities = allRecords.stream()
                .map(WeatherDataEntity::getCity)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        log.info("✅ Found {} unique cities", cities.size());
        return cities;
    }

    public Page<WeatherDataEntity> getWeatherByCity(String cityName, Pageable pageable) {
        log.info("🌤️  Fetching weather data for city: {}", cityName);
        return weatherDataRepository.findByCity(cityName, pageable);
    }

    public List<WeatherDataEntity> getLatestWeatherData(int limit) {
        log.info("⏰ Fetching latest {} weather records", limit);

        Pageable pageable = PageRequest.of(0, limit, Sort.by(Sort.Direction.DESC, "timestamp"));
        Page<WeatherDataEntity> page = weatherDataRepository.findAll(pageable);

        return page.getContent();
    }

    public Map<String, Object> getBasicStatistics() {
        log.info("📊 Calculating basic weather statistics");

        Map<String, Object> stats = new HashMap<>();

        try {
            // Total records
            long totalRecords = weatherDataRepository.count();
            stats.put("totalRecords", totalRecords);

            // Get all cities
            List<String> cities = getAllCities();
            stats.put("totalCities", cities.size());
            stats.put("cities", cities);

            // Get some sample data for calculations
            List<WeatherDataEntity> sampleData = weatherDataRepository.findAll();

            if (!sampleData.isEmpty()) {
                // Temperature statistics
                double avgTemp = sampleData.stream()
                        .mapToDouble(WeatherDataEntity::getTemperature)
                        .average()
                        .orElse(0.0);
                double minTemp = sampleData.stream()
                        .mapToDouble(WeatherDataEntity::getTemperature)
                        .min()
                        .orElse(0.0);
                double maxTemp = sampleData.stream()
                        .mapToDouble(WeatherDataEntity::getTemperature)
                        .max()
                        .orElse(0.0);

                stats.put("temperature", Map.of(
                        "average", Math.round(avgTemp * 100.0) / 100.0,
                        "minimum", Math.round(minTemp * 100.0) / 100.0,
                        "maximum", Math.round(maxTemp * 100.0) / 100.0
                ));

                // Rainfall statistics
                double totalRainfall = sampleData.stream()
                        .mapToDouble(WeatherDataEntity::getRainfall)
                        .sum();
                double avgRainfall = sampleData.stream()
                        .mapToDouble(WeatherDataEntity::getRainfall)
                        .average()
                        .orElse(0.0);

                stats.put("rainfall", Map.of(
                        "total", Math.round(totalRainfall * 100.0) / 100.0,
                        "average", Math.round(avgRainfall * 100.0) / 100.0
                ));

                // Humidity statistics
                double avgHumidity = sampleData.stream()
                        .mapToDouble(WeatherDataEntity::getHumidity)
                        .average()
                        .orElse(0.0);

                stats.put("humidity", Map.of(
                        "average", Math.round(avgHumidity * 100.0) / 100.0
                ));

                // Record counts
                long hotDays = sampleData.stream()
                        .mapToLong(data -> data.getTemperature() > 30.0 ? 1 : 0)
                        .sum();
                long rainyDays = sampleData.stream()
                        .mapToLong(data -> data.getRainfall() > 0.1 ? 1 : 0)
                        .sum();

                stats.put("counts", Map.of(
                        "hotDays", hotDays,
                        "rainyDays", rainyDays
                ));

                // Date range
                if (!sampleData.isEmpty()) {
                    LocalDateTime earliest = sampleData.stream()
                            .map(WeatherDataEntity::getTimestamp)
                            .min(LocalDateTime::compareTo)
                            .orElse(null);
                    LocalDateTime latest = sampleData.stream()
                            .map(WeatherDataEntity::getTimestamp)
                            .max(LocalDateTime::compareTo)
                            .orElse(null);

                    stats.put("dateRange", Map.of(
                            "earliest", earliest != null ? earliest.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : "unknown",
                            "latest", latest != null ? latest.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : "unknown"
                    ));
                }
            }

            stats.put("status", "SUCCESS");
            log.info("✅ Generated statistics for {} records", totalRecords);

        } catch (Exception e) {
            log.error("❌ Error calculating statistics", e);
            stats.put("status", "ERROR");
            stats.put("error", e.getMessage());
        }

        return stats;
    }

    public Page<WeatherDataEntity> searchWeatherData(String city, String startDate, String endDate,
                                                     Double minTemp, Double maxTemp, Pageable pageable) {
        log.info("🔍 Searching with filters: city={}, dates={}-{}, temp={}-{}",
                city, startDate, endDate, minTemp, maxTemp);

        // For simplicity, we'll implement basic filtering
        // In a real production system, you'd use Spring Data JPA Specifications or custom queries

        if (city != null && !city.trim().isEmpty()) {
            return weatherDataRepository.findByCity(city, pageable);
        }

        if (minTemp != null && maxTemp != null) {
            return weatherDataRepository.findByTemperatureBetween(minTemp, maxTemp, pageable);
        }

        if (minTemp != null) {
            return weatherDataRepository.findByTemperatureGreaterThanEqual(minTemp, pageable);
        }

        if (maxTemp != null) {
            return weatherDataRepository.findByTemperatureLessThanEqual(maxTemp, pageable);
        }

        // If no specific filters, return all
        return weatherDataRepository.findAll(pageable);
    }

    public String exportToCsv(String city, String startDate, String endDate) {
        log.info("📁 Exporting data to CSV: city={}, dates={}-{}", city, startDate, endDate);

        List<WeatherDataEntity> data;

        if (city != null && !city.trim().isEmpty()) {
            data = weatherDataRepository.findByCity(city);
        } else {
            // Limit export to avoid memory issues
            Pageable pageable = PageRequest.of(0, 1000, Sort.by(Sort.Direction.DESC, "timestamp"));
            data = weatherDataRepository.findAll(pageable).getContent();
        }

        StringBuilder csv = new StringBuilder();

        // CSV Header
        csv.append("timestamp,city,temperature,humidity,rainfall,windSpeed,pressure\n");

        // CSV Data
        for (WeatherDataEntity record : data) {
            csv.append(String.format("%s,%s,%.1f,%.1f,%.2f,%.1f,%.1f\n",
                    record.getTimestamp().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    record.getCity(),
                    record.getTemperature(),
                    record.getHumidity(),
                    record.getRainfall(),
                    record.getWindSpeed(),
                    record.getPressure()
            ));
        }

        log.info("✅ Exported {} records to CSV", data.size());
        return csv.toString();
    }

    // Helper method to get city-specific statistics
    public Map<String, Object> getCityStatistics(String cityName) {
        log.info("📊 Calculating statistics for city: {}", cityName);

        List<WeatherDataEntity> cityData = weatherDataRepository.findByCity(cityName);

        if (cityData.isEmpty()) {
            return Map.of("status", "ERROR", "message", "No data found for city: " + cityName);
        }

        Map<String, Object> stats = new HashMap<>();
        stats.put("city", cityName);
        stats.put("recordCount", cityData.size());

        double avgTemp = cityData.stream().mapToDouble(WeatherDataEntity::getTemperature).average().orElse(0.0);
        double avgHumidity = cityData.stream().mapToDouble(WeatherDataEntity::getHumidity).average().orElse(0.0);
        double totalRainfall = cityData.stream().mapToDouble(WeatherDataEntity::getRainfall).sum();

        stats.put("averageTemperature", Math.round(avgTemp * 100.0) / 100.0);
        stats.put("averageHumidity", Math.round(avgHumidity * 100.0) / 100.0);
        stats.put("totalRainfall", Math.round(totalRainfall * 100.0) / 100.0);
        stats.put("status", "SUCCESS");

        return stats;
    }
}