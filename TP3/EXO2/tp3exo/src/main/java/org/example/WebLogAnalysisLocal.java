package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class WebLogAnalysisLocal {

    // Simplified pattern for Apache log format
    private static final String LOG_PATTERN =
            "^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+).*";
    private static final Pattern PATTERN = Pattern.compile(LOG_PATTERN);

    // Class to represent parsed log entry
    public static class LogEntry implements Serializable {
        private String ip;
        private String timestamp;
        private String method;
        private String resource;
        private int httpCode;
        private long size;

        public LogEntry(String ip, String timestamp, String method, String resource, int httpCode, long size) {
            this.ip = ip;
            this.timestamp = timestamp;
            this.method = method;
            this.resource = resource;
            this.httpCode = httpCode;
            this.size = size;
        }

        // Getters
        public String getIp() { return ip; }
        public String getTimestamp() { return timestamp; }
        public String getMethod() { return method; }
        public String getResource() { return resource; }
        public int getHttpCode() { return httpCode; }
        public long getSize() { return size; }

        @Override
        public String toString() {
            return String.format("IP: %s, Method: %s, Resource: %s, Code: %d, Size: %d",
                    ip, method, resource, httpCode, size);
        }
    }

    // Function to parse log line
    private static LogEntry parseLogLine(String line) {
        try {
            Matcher matcher = PATTERN.matcher(line);
            if (matcher.matches()) {
                String ip = matcher.group(1);
                String timestamp = matcher.group(4);
                String method = matcher.group(5);
                String resource = matcher.group(6);
                int httpCode = Integer.parseInt(matcher.group(8));
                long size = Long.parseLong(matcher.group(9));

                return new LogEntry(ip, timestamp, method, resource, httpCode, size);
            }
        } catch (Exception e) {
            System.err.println("Failed to parse line: " + line);
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println("Starting Web Log Analysis...");
        System.out.println("Java version: " + System.getProperty("java.version"));

        // Set properties to avoid Hadoop warnings
        System.setProperty("hadoop.home.dir", "C:\\");  // Dummy path to avoid warnings

        SparkSession spark = null;
        try {
            // Create Spark session with local master
            spark = SparkSession
                    .builder()
                    .appName("WebLogAnalysisLocal")
                    .master("local[*]")
                    .config("spark.sql.adaptive.enabled", "false")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .getOrCreate();

            // Disable excessive logging
            spark.sparkContext().setLogLevel("WARN");

            System.out.println("Spark session created successfully!");

            // 1. Read data from local file system
            String filePath = "data/access.log";
            JavaRDD<String> logLines = spark.sparkContext()
                    .textFile(filePath, 1)
                    .toJavaRDD();

            long totalLines = logLines.count();
            System.out.println("=== WEB LOG ANALYSIS ===");
            System.out.println("Total lines read: " + totalLines);

            if (totalLines == 0) {
                System.out.println("ERROR: No data found in " + filePath);
                System.out.println("Please make sure the file exists and has data.");
                return;
            }

            // 2. Extract fields
            JavaRDD<LogEntry> parsedLogs = logLines
                    .map(WebLogAnalysisLocal::parseLogLine)
                    .filter(entry -> entry != null);

            long validEntries = parsedLogs.count();
            System.out.println("Successfully parsed entries: " + validEntries);

            if (validEntries == 0) {
                System.out.println("ERROR: No valid log entries found.");
                System.out.println("Please check the format of your access.log file.");
                return;
            }

            // Show sample entries
            System.out.println("\n=== SAMPLE ENTRIES ===");
            List<LogEntry> sampleEntries = parsedLogs.take(3);
            for (int i = 0; i < sampleEntries.size(); i++) {
                System.out.println((i + 1) + ". " + sampleEntries.get(i));
            }

            // 3. Basic statistics
            long totalRequests = parsedLogs.count();
            long errorRequests = parsedLogs.filter(entry -> entry.getHttpCode() >= 400).count();
            double errorPercentage = (double) errorRequests / totalRequests * 100;

            System.out.println("\n=== BASIC STATISTICS ===");
            System.out.println("Total requests: " + totalRequests);
            System.out.println("Error requests (HTTP ≥ 400): " + errorRequests);
            System.out.println("Error percentage: " + String.format("%.2f", errorPercentage) + "%");

            // 4. Top 5 IP addresses
            System.out.println("\n=== TOP 5 IP ADDRESSES ===");
            JavaPairRDD<String, Integer> ipCounts = parsedLogs
                    .mapToPair(entry -> new Tuple2<>(entry.getIp(), 1))
                    .reduceByKey(Integer::sum);

            List<Tuple2<Integer, String>> topIPs = ipCounts
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false)
                    .take(5);

            for (int i = 0; i < topIPs.size(); i++) {
                Tuple2<Integer, String> ipCount = topIPs.get(i);
                System.out.println((i + 1) + ". " + ipCount._2 + " - " + ipCount._1 + " requests");
            }

            // 5. Top 5 resources
            System.out.println("\n=== TOP 5 REQUESTED RESOURCES ===");
            JavaPairRDD<String, Integer> resourceCounts = parsedLogs
                    .mapToPair(entry -> new Tuple2<>(entry.getResource(), 1))
                    .reduceByKey(Integer::sum);

            List<Tuple2<Integer, String>> topResources = resourceCounts
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false)
                    .take(5);

            for (int i = 0; i < topResources.size(); i++) {
                Tuple2<Integer, String> resourceCount = topResources.get(i);
                System.out.println((i + 1) + ". " + resourceCount._2 + " - " + resourceCount._1 + " requests");
            }

            // 6. HTTP code distribution
            System.out.println("\n=== HTTP CODE DISTRIBUTION ===");
            JavaPairRDD<Integer, Integer> httpCodeCounts = parsedLogs
                    .mapToPair(entry -> new Tuple2<>(entry.getHttpCode(), 1))
                    .reduceByKey(Integer::sum);

            List<Tuple2<Integer, Integer>> httpCodes = httpCodeCounts.collect();

            for (Tuple2<Integer, Integer> httpCode : httpCodes) {
                System.out.println("Code " + httpCode._1 + ": " + httpCode._2 + " requests");
            }

            System.out.println("\n=== ANALYSIS COMPLETED SUCCESSFULLY ===");

        } catch (Exception e) {
            System.err.println("ERROR during analysis: " + e.getMessage());
            e.printStackTrace();
            System.err.println("\nTROUBLESHOOTING:");
            System.err.println("1. Use Java 17 instead of Java 24");
            System.err.println("2. Make sure data/access.log file exists");
            System.err.println("3. Check file permissions");
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }
}