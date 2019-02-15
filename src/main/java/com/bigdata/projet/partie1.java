package com.bigdata.projet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class partie1  {




	public static SparkSession newSession () {
		SparkSession Spark = SparkSession.builder().master("local[*]")
				.appName("BEKKI_KABORE_APP")
				.config("spark.master", "local")
				.getOrCreate();
		return Spark; 
	}

	public static Dataset<Row> readfile(String File_name, SparkSession spark) {
		Dataset<Row> dataset = spark.read()
				.option("delimiter", ",")
				.format("csv")
				.load(File_name)
				.toDF("temps", "utilisateur_source@domaine", "utilisateur_destination@domaine",
						"ordinateur_source", "ordinateur_destination", "type d'authentication",
						"type de connexion","orientation d'authentification","succès / échec");
		return dataset;}

}