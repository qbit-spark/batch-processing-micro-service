package com.qbitspark.dataingestionservice;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeatherRecord {

    private LocalDateTime timestamp;
    private String city;
    private Double temperature;
    private Double humidity;
    private Double rainfall;
    private Double windSpeed;
    private Double pressure;

    // Constructor from CSV line
    public static WeatherRecord fromCsvLine(String csvLine) {
        // Handle European decimal format - only replace commas between digits when followed by 1-2 digits
        // This preserves field separators while fixing decimal separators
        csvLine = csvLine.replaceAll("(\\d),(\\d{1,2})(?=,|$)", "$1.$2");

        String[] fields = csvLine.split(",");

        if (fields.length != 7) {
            throw new IllegalArgumentException("Invalid CSV line (expected 7 fields, got " + fields.length + "): " + csvLine);
        }

        try {
            return new WeatherRecord(
                    LocalDateTime.parse(fields[0].trim(),
                            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                    fields[1].trim(),
                    Double.parseDouble(fields[2].trim()),
                    Double.parseDouble(fields[3].trim()),
                    Double.parseDouble(fields[4].trim()),
                    Double.parseDouble(fields[5].trim()),
                    Double.parseDouble(fields[6].trim())
            );
        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing CSV line: " + csvLine, e);
        }
    }

    // Convert to JSON string for Kafka
    @Override
    public String toString() {
        return String.format(
                "{\"timestamp\":\"%s\",\"city\":\"%s\",\"temperature\":%.1f,\"humidity\":%.1f,\"rainfall\":%.2f,\"windSpeed\":%.1f,\"pressure\":%.1f}",
                timestamp, city, temperature, humidity, rainfall, windSpeed, pressure
        );
    }
}