/*
* La classe Partie contient des fonctions permettant de traiter les questions relatives à la partie 1 du projet.
*
*/


package com.bigdata.projet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class partie1  {

/* La fonction newSession permet de creer une nouvelle session Spark et definit les parametres d'environnement 
* de l'application. Le resultat de retour est une session. 
*/
	public static SparkSession newSession () {
		SparkSession Spark = SparkSession.builder().master("local[*]")
				.appName("BEKKI_KABORE_APP")
				.config("spark.master", "local")
				.getOrCreate();
		return Spark; 
	}
/* La fonction readfile permet de lire un fichier et le formater en dataset. Elle retourne un dataset the type Row.  
*/
	
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
