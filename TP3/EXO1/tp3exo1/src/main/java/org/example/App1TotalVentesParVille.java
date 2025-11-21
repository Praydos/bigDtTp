package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App1TotalVentesParVille {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("TotalVentesParVille")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("ventes.txt");

        // Extract (ville, prix)
        JavaPairRDD<String, Double> cityPricePairs = lines
                .mapToPair(line -> {
                    String[] parts = line.split("\\s+");
                    String ville = parts[1];
                    double prix = Double.parseDouble(parts[3]);
                    return new Tuple2<>(ville, prix);
                });

        // Reduce => total per city
        JavaPairRDD<String, Double> totalByCity = cityPricePairs
                .reduceByKey(Double::sum);

        System.out.println("\n====== TOTAL DES VENTES PAR VILLE ======\n");
        totalByCity.collect().forEach(System.out::println);

        sc.close();
    }
}
