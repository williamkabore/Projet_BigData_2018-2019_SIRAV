package com.bigdata.projet;

import static org.apache.spark.sql.functions.*;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class main {


	public static void main(String[] args) {
		
		
		/**************************SPARK Config************************************/
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		System.setProperty("hadoop.home.dir", "V:/winutils");
		System.setProperty("SPARK_LOCAL_IP","127.0.0.1");

		SparkSession spark = SparkSession.builder() .master("local[*]")
							.appName("BEKKI_KABORE_APP").config("spark.master", "local")
							.getOrCreate();
		
		System.out.println("************************************** Partie I ************************************");
		System.out.println("1. Lecture du fichier logs...");
		Dataset<Row> ds = partie1.readfile ("auth_500000.txt",spark);
		System.out.println("2. Suppression des lignes de logs qui contiennent le symbole ' ?'");
		Dataset<Row> dsClean =  ds.filter(line -> !line.toString().contains("?"));	
		//dsClean.show(10);
        
		System.out.println(" 3. Nombre d'utilisation d'une machine (ordinateur_source) par un utilisateur (utilisateur_source@domaine)");
        Dataset<Row> Filter = dsClean.withColumn("(utilisateur_source@domaine, ordinateur_source)", concat(dsClean.col("utilisateur_source@domaine"),
                lit(",   "), dsClean.col("ordinateur_source")));
        //Filter.groupBy("(utilisateur_source@domaine, ordinateur_source)" ).count().show(10);
        
        System.out.println(" 4. Affichage du top 10 des accès les plus fréquents");
        //Filter.groupBy("(utilisateur_source@domaine, ordinateur_source)" ).count().orderBy(org.apache.spark.sql.functions.col("count").desc()).show(10);

        
        System.out.println("************************************** Partie II ************************************");
        System.out.println("1.a. Nombre de connexions effectuées sur une machine source (ordinateur_source) vers une machine destination (ordinateur_destination) pour chaque utilisateur (utilisateur_source@domaine)");
        Dataset<Row> Connexion  = partie2.GroupConnex(dsClean, "utilisateur_source@domaine", "ordinateur_source","ordinateur_destination", "Connexion");
        //Connexion.show(10);

        System.out.println("1.b. Dataframe contenant les utilisateurs (utilisateur_source@domaine) et les pairs (ordinateur_source, ordinateur_destination)");
        Dataset<Row> User_Connex = partie2.ColUnion(Connexion, "utilisateur_source@domaine", "Connexion", "utilisateurs et connexions");
		//User_Connex.toDF("Utilisateurs et connexions").show(10);

        System.out.println("2.a. Nombre de d'authentifications (Logon,...) avec ou sans succès (succès / échec) pour chaque utilisateur");
        Dataset<Row> UserLogon = partie2.GroupConnex(dsClean, "utilisateur_source@domaine", "orientation d'authentification","succès / échec", "Connexion2");
        //UserLogon.toDF("Utilisateurs","Connexions","Poids").show(10);
              
        System.out.println("2.b. Dataframe contenant les utilisateurs (utilisateur_source@domaine) et les pairs (Logon,...) avec ou sans succès (succès / échec)");
        Dataset<Row> User_Logon  = partie2.ColUnion(UserLogon, "utilisateur_source@domaine", "Connexion2", "utilisateurs et statue de connexions");
        User_Logon.toDF("Utilisateurs et connexions").write().text("V:/Test/2b");
  
        System.out.println("3.a. calculons le nombre de d'utilisateurs (utilisateur_source@domaine) avec ou sans succès (succès / échec) pour chaque machine source (ordinateur_source)");
        Dataset<Row> HostLogon = partie2.GroupConnex(dsClean, "utilisateur_source@domaine", "utilisateur_source@domaine","succès / échec", "Host_Connexion");
        //HostLogon.toDF("Utilisateurs","Connexions","Poids").show(10);
       
        System.out.println("3.b. Dataframe contenant les utilisateurs (utilisateur_source@domaine) et les pairs (ordinateur_source, ordinateur_destination)");
        Dataset<Row> Host_Logon  = partie2.ColUnion(HostLogon, "utilisateur_source@domaine", "Host_Connexion", "Machine et statue de connexions");
        //Host_Logon.toDF("Utilisateurs et connexions").show(10); 
        
        // Répertoire de sauvegarde des fichiers à entrer lors de l'exécution
        String outputdir = args[0];
        
        System.out.println("*****************Partie III ***********************");  
		//partie3.GroupConnex(dsClean, "Connex", outputdir); 

		System.out.println("*****************Partie IV ***********************"); 
		//partie4.TempWindow(dsClean, "Connex", outputdir);


}

}
