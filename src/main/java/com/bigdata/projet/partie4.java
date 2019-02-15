package com.bigdata.projet;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class partie4 {
        /*
	* TempWindow est une fonction qui utilise les fenêtres temporelles, pour filter le dataset
	* Le resultat est ensuite sauvegardé vers des fichiers JSON.
	* Le paramètre "outputdir" spécifie le chemin du répertoire de destination
	*/
	static int t, i,j,k=0,ft=60;

	public static void TempWindow (Dataset<Row> ds,String Connex, String outputdir) {
		Dataset<Row> Grouped = null;
		int max=ds.schema().length();
		String [] column = ds.columns();

		Dataset<Row> temps =ds.filter(column[0]+" <= "+ft+" AND "+column[0]+">0");
		while (temps.count()!=0){

			for (i=1; i<max ; i++) {
				for (j=1; j<max ; j++) {
					if (i!=j)
						for (k=j+1; k<max; k++) {
							if(k!=i && k!=j)
								Grouped=ds.withColumn(Connex, concat(ds.col(column[j]),
										lit(","), ds.col(column[k])))
								.groupBy(column[i],Connex).count()
								.orderBy(org.apache.spark.sql.functions.col("count").desc());
							Grouped.write().json(outputdir+Integer.toString(ft)+Integer.toString(i)+Integer.toString(j)+Integer.toString(k));
						}
				}
			}
			/*Incrémentation de la fenêtre temporelle  par 60 seconde*/
			ft+=60;
		}
	}
}
