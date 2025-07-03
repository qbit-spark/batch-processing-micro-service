package com.qbitspark.dataprocessingservice;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/analytics")
@Slf4j
public class WeatherAnalyticsController {

    @Autowired
    private WeatherAnalyticsService weatherAnalyticsService;

    private volatile boolean isProcessing = false;
    private Map<String, Object> lastReport = null;
    private LocalDateTime lastReportTime = null;

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Weather Analytics Processing Service");
        response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        response.put("sparkEnabled", false);
        response.put("isProcessing", isProcessing);

        if (lastReportTime != null) {
            response.put("lastReportGenerated", lastReportTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        }

        return ResponseEntity.ok(response);
    }

    @PostMapping("/quarterly")
    public ResponseEntity<Map<String, Object>> generateQuarterlyReport() {
        log.info("🚀 Received request to generate quarterly analytics report");

        Map<String, Object> response = new HashMap<>();

        if (isProcessing) {
            response.put("status", "ALREADY_PROCESSING");
            response.put("message", "Analytics generation is already in progress");
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            return ResponseEntity.status(409).body(response); // Conflict
        }

        try {
            // Run analytics generation asynchronously
            CompletableFuture.runAsync(() -> {
                isProcessing = true;
                try {
                    log.info("🔥 Starting quarterly analytics generation...");
                    Map<String, Object> report = weatherAnalyticsService.generateQuarterlyReport();

                    // Store the report
                    lastReport = report;
                    lastReportTime = LocalDateTime.now();

                    log.info("✅ Quarterly analytics report generated successfully!");

                } catch (Exception e) {
                    log.error("❌ Error generating quarterly report", e);
                } finally {
                    isProcessing = false;
                }
            });

            response.put("status", "STARTED");
            response.put("message", "Quarterly analytics generation started successfully");
            response.put("estimatedTime", "5-10 minutes for 2.3M records");
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            response.put("checkStatusUrl", "/api/analytics/status");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error starting quarterly analytics generation", e);

            response.put("status", "ERROR");
            response.put("message", "Failed to start analytics generation: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getProcessingStatus() {
        Map<String, Object> response = new HashMap<>();

        response.put("isProcessing", isProcessing);
        response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        if (isProcessing) {
            response.put("status", "PROCESSING");
            response.put("message", "Analytics generation is currently in progress");
        } else if (lastReport != null) {
            response.put("status", "COMPLETED");
            response.put("message", "Latest analytics report is available");
            response.put("lastReportGenerated", lastReportTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            response.put("reportDataUrl", "/api/analytics/report");
        } else {
            response.put("status", "READY");
            response.put("message", "Ready to generate analytics report");
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/report")
    public ResponseEntity<Map<String, Object>> getLatestReport() {
        if (lastReport == null) {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "NO_REPORT");
            response.put("message", "No analytics report available. Generate one first.");
            response.put("generateUrl", "/api/analytics/quarterly");
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.status(404).body(response);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("reportGenerated", lastReportTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        response.put("data", lastReport);
        response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        return ResponseEntity.ok(response);
    }

    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processUnprocessedRecords() {
        log.info("🔄 Received request to process unprocessed records");

        Map<String, Object> response = new HashMap<>();

        try {
            Map<String, Object> result = weatherAnalyticsService.processUnprocessedRecords();

            response.put("status", "SUCCESS");
            response.put("result", result);
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error processing unprocessed records", e);

            response.put("status", "ERROR");
            response.put("message", "Failed to process unprocessed records: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            return ResponseEntity.status(500).body(response);
        }
    }

    @GetMapping("/summary")
    public ResponseEntity<Map<String, Object>> getAnalyticsSummary() {
        Map<String, Object> response = new HashMap<>();

        response.put("service", "Weather Analytics Processing");
        response.put("description", "Spring batch-powered analytics for Tanzania weather data");
        response.put("capabilities", java.util.Arrays.asList(
                "Quarterly weather analytics",
                "City-wise pattern analysis",
                "Temperature and rainfall trends",
                "Extreme weather event detection",
                "Monthly trend analysis",
                "2.3M+ record processing"
        ));

        response.put("endpoints", java.util.Map.of(
                "health", "GET /api/analytics/health",
                "generateReport", "POST /api/analytics/quarterly",
                "getStatus", "GET /api/analytics/status",
                "getReport", "GET /api/analytics/report",
                "processRecords", "POST /api/analytics/process"
        ));

        response.put("sparkConfiguration", java.util.Map.of(
                "appName", "Tanzania Weather Data Analytics",
                "master", "local[*]",
                "memoryAllocation", "2GB driver + 2GB executor"
        ));

        response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        return ResponseEntity.ok(response);
    }

    @PostMapping("/clear-cache")
    public ResponseEntity<Map<String, Object>> clearReportCache() {
        log.info("🗑️ Clearing analytics report cache");

        lastReport = null;
        lastReportTime = null;

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "Analytics report cache cleared");
        response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        return ResponseEntity.ok(response);
    }
}