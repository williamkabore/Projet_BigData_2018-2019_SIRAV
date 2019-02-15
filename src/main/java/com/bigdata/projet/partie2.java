package com.bigdata.projet;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class partie2 {
 
	public static Dataset<Row> fselect (Dataset<Row> ds, String col1, String col2, String col3) {
	
		Dataset<Row> selected= ds.select(ds.col(col1),
	        		            ds.col(col2),
	        		            ds.col(col3));
		return selected;

	}
	
	public static Dataset<Row> GroupConnex (Dataset<Row> ds, String col1, String col2, String col3,String Connex) {
		Dataset<Row> Grouped = ds.withColumn(Connex, concat(ds.col(col2),
				                 lit(","), ds.col(col3)))
				                 .groupBy(col1,Connex).count()
				                 .orderBy(org.apache.spark.sql.functions.col("count").desc());
		return Grouped;

	}
	public static Dataset<Row> ColUnion (Dataset<Row> ds, String col1, String col2, String name) {
	    Dataset<Row> Union = ds.select(col1)
				  			 .union(ds.select(col2)).distinct()
				  			 .toDF(name);
		return Union;

	}
	
}
