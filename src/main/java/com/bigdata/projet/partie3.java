package com.bigdata.projet;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class partie3 {

	static int i,j,k=0;

	public static void GroupConnex (Dataset<Row> ds,String Connex, String outputdir) {
		Dataset<Row> Grouped = null;
		//String outputdir = "V:/Test/output/output_partie3";
		int max=ds.schema().length();
		String [] column = ds.columns();
		System.out.println(ds.col("temps"));
		for (i=1; i<max ; i++) {
			for (j=1; j<max ; j++) {
				if (i!=j)
					for (k=j+1; k<max; k++) {
						if(k!=i && k!=j)
							System.out.println("pair ==> : (" +Integer.toString(i)+" "+"("+Integer.toString(j)+Integer.toString(k)+"))" );
						Grouped=ds.withColumn(Connex, concat(ds.col(column[j]),
								lit(","), ds.col(column[k])))
								.groupBy(column[i],Connex).count()
								.orderBy(org.apache.spark.sql.functions.col("count").desc());
						Grouped.write().json(outputdir+Integer.toString(i)+Integer.toString(j)+Integer.toString(k));                             }
			}
		}
	}


}