package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

public class App2TotalVentesVilleAnnee {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("TotalVentesParVilleEtAnnee")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder()
                .appName("SQLVentes")
                .getOrCreate();

        StructType schema = new StructType()
                .add("date", DataTypes.StringType)
                .add("ville", DataTypes.StringType)
                .add("produit", DataTypes.StringType)
                .add("prix", DataTypes.DoubleType);

        Dataset<Row> df = spark.read()
                .schema(schema)
                .option("delimiter", " ")
                .csv("ventes.txt");

        df = df.withColumn("annee", functions.year(functions.to_date(df.col("date"))));

        df.createOrReplaceTempView("ventes");

        Dataset<Row> result = spark.sql(
                "SELECT ville, annee, SUM(prix) AS total_ventes " +
                        "FROM ventes GROUP BY ville, annee ORDER BY annee, ville"
        );

        System.out.println("\n====== TOTAL DES VENTES PAR VILLE ET PAR ANNEE ======\n");
        result.show();

        sc.close();
        spark.close();
    }
}
