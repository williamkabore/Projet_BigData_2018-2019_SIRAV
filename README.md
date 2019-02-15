#### Auteurs : Aziz BEKKI et William KABORE
Lien vers le projet [GitHub].
# Projet Big data 2018-2019-Master : SIRAV
## Partie I
### 1. Lecture du fichier logs.
```sh
Dataset<Row> ds = partie1.readfile ("auth_500000.txt",spark);
```
La fonction méthode est definie comme suit:
```sh
	public static Dataset<Row> readfile(String File_name, SparkSession spark) {
		Dataset<Row> dataset = spark.read()
				  .option("delimiter", ",")
				  .format("csv")
				  .load(File_name)
				  .toDF("temps", "utilisateur_source@domaine", "utilisateur_destination@domaine",
					     "ordinateur_source", "ordinateur_destination", "type d'authentication",
					     "type de connexion","orientation d'authentification","succès / échec");
		return dataset;
```
### 2. Suppression des lignes de logs qui contiennent le symbole ' ?'.
```sh
+-----+--------------------------+-------------------------------+-----------------+----------------------+---------------------+-----------------+------------------------------+--------------+
|temps|utilisateur_source@domaine|utilisateur_destination@domaine|ordinateur_source|ordinateur_destination|type d'authentication|type de connexion|orientation d'authentification|succès / échec|
+-----+--------------------------+-------------------------------+-----------------+----------------------+---------------------+-----------------+------------------------------+--------------+
|    1|      ANONYMOUS LOGON@C586|           ANONYMOUS LOGON@C586|            C1250|                  C586|                 NTLM|          Network|                         LogOn|       Success|
|    1|               C1020$@DOM1|                   SYSTEM@C1020|            C1020|                 C1020|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C1021$@DOM1|                    C1021$@DOM1|            C1021|                  C625|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1035$@DOM1|                    C1035$@DOM1|            C1035|                  C586|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1069$@DOM1|                   SYSTEM@C1069|            C1069|                 C1069|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C1085$@DOM1|                    C1085$@DOM1|            C1085|                  C612|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1151$@DOM1|                   SYSTEM@C1151|            C1151|                 C1151|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C1154$@DOM1|                   SYSTEM@C1154|            C1154|                 C1154|            Negotiate|          Service|                         LogOn|       Success|
|    1|                C119$@DOM1|                     C119$@DOM1|             C119|                  C528|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1218$@DOM1|                    C1218$@DOM1|            C1218|                  C529|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1241$@DOM1|                   SYSTEM@C1241|            C1241|                 C1241|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C1250$@DOM1|                    C1250$@DOM1|            C1250|                  C586|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1314$@DOM1|                    C1314$@DOM1|            C1314|                  C467|             Kerberos|          Network|                         LogOn|       Success|
|    1|                C144$@DOM1|                    SYSTEM@C144|             C144|                  C144|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C1444$@DOM1|                    C1444$@DOM1|            C1444|                  C528|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1492$@DOM1|                    C1492$@DOM1|            C1492|                  C467|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1492$@DOM1|                    C1492$@DOM1|            C1492|                  C528|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1492$@DOM1|                    C1492$@DOM1|            C1492|                  C586|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1492$@DOM1|                    C1492$@DOM1|            C1798|                 C1492|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1504$@DOM1|                      U45@C1504|            C1504|                 C1504|            Negotiate|            Batch|                         LogOn|       Success|
|    1|               C1543$@DOM1|                   SYSTEM@C1543|            C1543|                 C1543|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C1727$@DOM1|                    C1727$@DOM1|            C1727|                 C1881|             Kerberos|          Network|                         LogOn|       Success|
|    1|                C175$@DOM1|                    SYSTEM@C175|             C175|                  C175|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C1767$@DOM1|                       U86@DOM1|            C1767|                 C1767|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1920$@DOM1|                    C1920$@DOM1|            C1920|                  C586|             Kerberos|          Network|                         LogOn|       Success|
|    1|               C1934$@DOM1|                   SYSTEM@C1934|            C1934|                 C1934|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C1975$@DOM1|                   SYSTEM@C1975|            C1975|                 C1975|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C2067$@DOM1|                   SYSTEM@C2067|            C2067|                 C2067|            Negotiate|          Service|                         LogOn|       Success|
|    1|               C2095$@DOM1|                   SYSTEM@C2095|            C2095|                 C2095|            Negotiate|          Service|                         LogOn|       Success|
|    1|                C210$@DOM1|                    SYSTEM@C210|             C210|                  C210|            Negotiate|          Service|                         LogOn|       Success|
+-----+--------------------------+-------------------------------+-----------------+----------------------+---------------------+-----------------+------------------------------+--------------+
```

### 3. Nombre d'utilisation d'une machine (ordinateur_source) par un utilisateur (utilisateur_source@domaine).
```sh
+-----------------------------------------------+-----+
|(utilisateur_source@domaine, ordinateur_source)|count|
+-----------------------------------------------+-----+
|                           C1055$@DOM1,   C1055|   10|
|                           C2701$@DOM1,   C2701|   12|
|                           C2751$@DOM1,   C2751|   22|
|                             C367$@DOM1,   C367|   15|
|                           C4328$@DOM1,   C3896|   32|
|                           C1242$@DOM1,   C1242|   13|
|                           C1412$@DOM1,   C1412|   21|
|                           C1372$@DOM1,   C1372|    7|
|                             C382$@DOM1,   C382|   26|
|                           C2279$@DOM1,   C2280|  158|
+-----------------------------------------------+-----+
```
### 4. Affichage du top 10 des accès les plus fréquents.
```sh
+-----------------------------------------------+-----+
|(utilisateur_source@domaine, ordinateur_source)|count|
+-----------------------------------------------+-----+
|                            C599$@DOM1,   C1619| 3837|
|                             C585$@DOM1,   C585| 3384|
|                           C1114$@DOM1,   C1115| 2929|
|                             C743$@DOM1,   C743| 2925|
|                             C104$@DOM1,   C105| 2725|
|                             C567$@DOM1,   C574| 2460|
|                             C123$@DOM1,   C527| 2345|
|                           C1617$@DOM1,   C1618| 2002|
|                             C538$@DOM1,   C539| 1930|
|                               U22@DOM1,   C506| 1907|
+-----------------------------------------------+-----+
only showing top 10 rows
```

## Partie II
### 1. Nombre de connexions effectuées sur une machine source (ordinateur_source) vers une machine destination (ordinateur_destination) pour chaque utilisateur (utilisateur_source@domaine).
#### a. Sauvegarde du résultat dans une DataFrame
```sh
+--------------------------+-----------+-----+
|                C585$@DOM1|  C585,C586| 3376|
|                C743$@DOM1|  C743,C586| 2898|
|                C480$@DOM1|  C480,C625| 1670|
|                  U19@DOM1|  C229,C229| 1336|
|                  U48@DOM1|  C419,C419| 1054|
|                C599$@DOM1| C1619,C101| 1037|
|                  U34@DOM1|  C921,C921| 1029|
|                C529$@DOM1|  C529,C529| 1001|
|                  U53@DOM1|C1710,C1710|  976|
|                C599$@DOM1| C1619,C553|  917|
+--------------------------+-----------+-----+
only showing top 10 rows
```
#### b. Dataframe contenant les utilisateurs (utilisateur_source@domaine) et les pairs (ordinateur_source, ordinateur_destination)
```sh
+--------------------------+
|Utilisateurs et connexions|
+--------------------------+
|               C2962$@DOM1|
|               C3608$@DOM1|
|               C2470$@DOM1|
|               C4248$@DOM1|
|               C2645$@DOM1|
|               C3844$@DOM1|
|               C1800$@DOM1|
|               C2641$@DOM1|
|               C3787$@DOM1|
|               C3572$@DOM1|
|               C3232$@DOM1|
|               C3703$@DOM1|
|                 C64$@DOM1|
|               C3542$@DOM1|
|               C5107$@DOM1|
|               C1775$@DOM1|
|                C254$@DOM1|
|                C990$@DOM1|
|               C5020$@DOM1|
|               C1482$@DOM1|
|                C261$@DOM1|
|               C4513$@DOM1|
|               C1210$@DOM1|
|                U1051@DOM1|
|                 U150@DOM1|
|                 U875@DOM1|
|                U1336@DOM1|
|               C1485,C2106|
|                C1069,C528|
|                C3659,C625|
|                C2846,C586|
|                C1215,C529|
|               C3863,C1065|
|                 C360,C528|
|                C617,C1065|
|                 C234,C625|
|                 C800,C529|
|                C410,C2106|
|               C1455,C1455|
|                C1352,C457|
|                C3709,C457|
|               C2463,C2106|
|                C4102,C612|
+--------------------------+
```
### 2. Nombre de d'authentifications (Logon,...) avec ou sans succès (succès / échec) pour chaque utilisateur
#### a. Sauvegarde du résultat dans une DataFrame
```sh
+--------------------+-------------+-----+
|        Utilisateurs|   Connexions|Poids|
+--------------------+-------------+-----+
|            U22@DOM1|LogOn,Success| 6342|
|ANONYMOUS LOGON@C586|LogOn,Success| 5907|
|            U66@DOM1|LogOn,Success| 4885|
|          C599$@DOM1|LogOn,Success| 3837|
|          C585$@DOM1|LogOn,Success| 3384|
|         C1114$@DOM1|LogOn,Success| 2929|
|          C743$@DOM1|LogOn,Success| 2925|
|          C104$@DOM1|LogOn,Success| 2725|
|          C567$@DOM1|LogOn,Success| 2460|
|          C123$@DOM1|LogOn,Success| 2345|
+--------------------+-------------+-----+
only showing top 10 rows
```
#### b. Dataframe contenant les utilisateurs (utilisateur_source@domaine) et les pairs (Logon,...) avec ou sans succès (succès / échec)
```sh

```
### 3. calculons le nombre de d'utilisateurs (utilisateur_source@domaine) avec ou sans succès (succès / échec) pour chaque machine source (ordinateur_source)
#### Sauvegarde du résultat dans une DataFrame
```sh
+--------------------+--------------------+-----+
|        Utilisateurs|          Connexions|Poids|
+--------------------+--------------------+-----+
|            U22@DOM1|    U22@DOM1,Success| 6342|
|ANONYMOUS LOGON@C586|ANONYMOUS LOGON@C...| 5907|
|            U66@DOM1|    U66@DOM1,Success| 4885|
|          C599$@DOM1|  C599$@DOM1,Success| 3837|
|          C585$@DOM1|  C585$@DOM1,Success| 3384|
|         C1114$@DOM1| C1114$@DOM1,Success| 2929|
|          C743$@DOM1|  C743$@DOM1,Success| 2925|
|          C104$@DOM1|  C104$@DOM1,Success| 2725|
|          C567$@DOM1|  C567$@DOM1,Success| 2460|
|          C123$@DOM1|  C123$@DOM1,Success| 2345|
+--------------------+--------------------+-----+
only showing top 10 rows
```
#### Dataframe contenant les utilisateurs (utilisateur_source@domaine) et les pairs (ordinateur_source, ordinateur_destination)
```sh
+--------------------------+
|Utilisateurs et connexions|
+--------------------------+
|               C4248$@DOM1|
|               C3608$@DOM1|
|                C254$@DOM1|
|               C2645$@DOM1|
|               C3542$@DOM1|
|                 C64$@DOM1|
|               C3572$@DOM1|
|               C3787$@DOM1|
|               C3844$@DOM1|
|               C1210$@DOM1|
|               C2962$@DOM1|
|               C3703$@DOM1|
|               C2470$@DOM1|
|               C1775$@DOM1|
|               C2641$@DOM1|
|               C3232$@DOM1|
|               C1800$@DOM1|
|               C1482$@DOM1|
|               C5107$@DOM1|
|               C5020$@DOM1|
|                C990$@DOM1|
|                C261$@DOM1|
|                U1051@DOM1|
|               C4513$@DOM1|
|                 U934@DOM1|
|                 U875@DOM1|
|                U1336@DOM1|
|                 U150@DOM1|
|      NETWORK SERVICE@C...|
|      NETWORK SERVICE@C...|
|      NETWORK SERVICE@C...|
|       LOCAL SERVICE@C5538|
|      ANONYMOUS LOGON@C...|
|       C1837$@DOM1,Success|
|       C1495$@DOM1,Success|
|        C272$@DOM1,Success|
|       C2754$@DOM1,Success|
|        C910$@DOM1,Success|
|          C5$@DOM1,Success|
|         U676@DOM1,Success|
|          C1$@DOM1,Success|
|         C69$@DOM1,Success|
|        C932$@DOM1,Success|
|       C3595$@DOM1,Success|
|       C1790$@DOM1,Success|
|        U1415@DOM1,Success|
|        U1022@DOM1,Success|
|         U872@DOM1,Success|
|        U1268@DOM1,Success|
+--------------------------+
```
## Partie III
```sh

```
## Partie IV
```sh

```


[GitHub]:<https://github.com/williamkabore/Projet_BigData_2018-2019_SIRAV>
![alt text](https://github.com/AzizGS/BigData/blob/master/Capture.PNG)
