package com.qbitspark.datadeliveryservice;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/weather")
@Slf4j
public class WeatherDataController {

    @Autowired
    private WeatherDataService weatherDataService;

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Data Delivery Service");
        response.put("description", "REST API for weather data access");
        response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        // Add some basic stats
        try {
            long totalRecords = weatherDataService.getTotalRecords();
            response.put("totalRecords", totalRecords);
            response.put("databaseConnected", true);
        } catch (Exception e) {
            response.put("databaseConnected", false);
            response.put("error", e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/cities")
    public ResponseEntity<Map<String, Object>> getCities() {
        log.info("📍 Fetching available cities");

        try {
            List<String> cities = weatherDataService.getAllCities();

            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("cities", cities);
            response.put("cityCount", cities.size());
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error fetching cities", e);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "ERROR");
            response.put("message", "Failed to fetch cities: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/city/{cityName}")
    public ResponseEntity<Map<String, Object>> getWeatherByCity(
            @PathVariable String cityName,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size,
            @RequestParam(defaultValue = "timestamp") String sortBy,
            @RequestParam(defaultValue = "desc") String sortDir) {

        log.info("🌤️  Fetching weather data for city: {} (page: {}, size: {})", cityName, page, size);

        try {
            Sort sort = Sort.by(sortDir.equals("desc") ? Sort.Direction.DESC : Sort.Direction.ASC, sortBy);
            Pageable pageable = PageRequest.of(page, size, sort);

            Page<WeatherDataEntity> weatherPage = weatherDataService.getWeatherByCity(cityName, pageable);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("city", cityName);
            response.put("data", weatherPage.getContent());
            response.put("pagination", Map.of(
                    "currentPage", page,
                    "totalPages", weatherPage.getTotalPages(),
                    "totalElements", weatherPage.getTotalElements(),
                    "size", size,
                    "hasNext", weatherPage.hasNext(),
                    "hasPrevious", weatherPage.hasPrevious()
            ));
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error fetching weather data for city: {}", cityName, e);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "ERROR");
            response.put("message", "Failed to fetch data for city " + cityName + ": " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/latest")
    public ResponseEntity<Map<String, Object>> getLatestWeatherData(
            @RequestParam(defaultValue = "10") int limit) {

        log.info("⏰ Fetching latest {} weather records", limit);

        try {
            List<WeatherDataEntity> latestData = weatherDataService.getLatestWeatherData(limit);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("data", latestData);
            response.put("count", latestData.size());
            response.put("limit", limit);
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error fetching latest weather data", e);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "ERROR");
            response.put("message", "Failed to fetch latest data: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getBasicStats() {
        log.info("📊 Fetching basic weather statistics");

        try {
            Map<String, Object> stats = weatherDataService.getBasicStatistics();

            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("statistics", stats);
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error fetching statistics", e);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "ERROR");
            response.put("message", "Failed to fetch statistics: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/search")
    public ResponseEntity<Map<String, Object>> searchWeatherData(
            @RequestParam(required = false) String city,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) Double minTemp,
            @RequestParam(required = false) Double maxTemp,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {

        log.info("🔍 Searching weather data with filters: city={}, dates={} to {}, temp={} to {}",
                city, startDate, endDate, minTemp, maxTemp);

        try {
            Pageable pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "timestamp"));
            Page<WeatherDataEntity> results = weatherDataService.searchWeatherData(
                    city, startDate, endDate, minTemp, maxTemp, pageable);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("data", results.getContent());
            response.put("searchCriteria", Map.of(
                    "city", city != null ? city : "all",
                    "startDate", startDate != null ? startDate : "any",
                    "endDate", endDate != null ? endDate : "any",
                    "minTemp", minTemp != null ? minTemp : "any",
                    "maxTemp", maxTemp != null ? maxTemp : "any"
            ));
            response.put("pagination", Map.of(
                    "currentPage", page,
                    "totalPages", results.getTotalPages(),
                    "totalElements", results.getTotalElements(),
                    "size", size
            ));
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error searching weather data", e);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "ERROR");
            response.put("message", "Search failed: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/export/csv")
    public ResponseEntity<Map<String, Object>> exportToCsv(
            @RequestParam(required = false) String city,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {

        log.info("📁 Exporting weather data to CSV: city={}, dates={} to {}", city, startDate, endDate);

        try {
            String csvData = weatherDataService.exportToCsv(city, startDate, endDate);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("format", "CSV");
            response.put("data", csvData);
            response.put("exportCriteria", Map.of(
                    "city", city != null ? city : "all",
                    "startDate", startDate != null ? startDate : "any",
                    "endDate", endDate != null ? endDate : "any"
            ));
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error exporting to CSV", e);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "ERROR");
            response.put("message", "Export failed: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/summary")
    public ResponseEntity<Map<String, Object>> getServiceSummary() {
        Map<String, Object> response = new HashMap<>();

        response.put("service", "Weather Data Delivery Service");
        response.put("description", "REST API for accessing and exporting Tanzania weather data");
        response.put("version", "1.0.0");
        response.put("port", 8084);

        response.put("endpoints", Map.of(
                "health", "GET /api/weather/health - Service health check",
                "cities", "GET /api/weather/cities - List all available cities",
                "cityData", "GET /api/weather/city/{cityName} - Get weather data for specific city",
                "latest", "GET /api/weather/latest - Get latest weather records",
                "stats", "GET /api/weather/stats - Basic weather statistics",
                "search", "GET /api/weather/search - Search with filters",
                "export", "GET /api/weather/export/csv - Export data as CSV"
        ));

        response.put("features", List.of(
                "Paginated data access",
                "City-specific filtering",
                "Date range filtering",
                "Temperature range filtering",
                "CSV export functionality",
                "Real-time statistics",
                "Latest data access"
        ));

        response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        return ResponseEntity.ok(response);
    }
}

//API Endpoints:
//
//GET /api/weather/health - Service health check
//GET /api/weather/cities - List all available cities
//GET /api/weather/city/{cityName} - Get weather data for specific city (paginated)
//GET /api/weather/latest - Get latest weather records
//GET /api/weather/stats - Basic weather statistics
//GET /api/weather/search - Search with filters (city, date, temperature)
//GET /api/weather/export/csv - Export data as CSV
//GET /api/weather/summary - Service information and capabilities
