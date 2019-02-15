package com.bigdata.projet;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class partie2 {
 
	/*La fonction fselect permet de selectionner 3 colones du dataset initial*/
	public static Dataset<Row> fselect (Dataset<Row> ds, String col1, String col2, String col3) {
	
		Dataset<Row> selected= ds.select(ds.col(col1),
	        		            ds.col(col2),
	        		            ds.col(col3));
		return selected;

	}
	/*La fonction GroupConnex, prend en paramètres 3 colonnes et un string, elle crée une nouvelle colones appelée
	* Connex, qui contiendra une paire constituée des deux colonnes col2 et col3
	* Le resultat de retour est un dataset
	*/
	public static Dataset<Row> GroupConnex (Dataset<Row> ds, String col1, String col2, String col3,String Connex) {
		Dataset<Row> Grouped = ds.withColumn(Connex, concat(ds.col(col2),
				                 lit(","), ds.col(col3)))
				                 .groupBy(col1,Connex).count()
				                 .orderBy(org.apache.spark.sql.functions.col("count").desc());
		return Grouped;

	}
	/*La fonction ColUnion, utilise la methode Union pour fusioner les deux colones du dataset précédent.
	*/
	  public static Dataset<Row> ColUnion (Dataset<Row> ds, String col1, String col2, String name) {
	  Dataset<Row> Union = ds.select(col1)
	 /*On limite la selection à 10 lignes pour la premiere colonne puis dans le main on affiche les 20 premieres lignes
	  pour permettre d'afficher de bien constater la fusion des deux colonnes*/   					 
	    					 .limit(10)
				  		 .union(ds.select(col2)).distinct()
				  		 .toDF(name);
	   return Union;
	   }
	
}
