# Market-Basket-Analysis

This work was done as part of INF-553 (Foundatiosn and Applicatiosn of Data Mining) coursework at USC

• Implemented SON and Apriori algorithms for finding pairs of movies that are frequently (that is, greater than a certain support threshold) rated together by users. This was built using Python, and on top of Apache Spark framework. Extended the implementation further to find frequent triples, quadruples, and so on.

• I used three different datasets, ranging from very small to very large.

• Case1 calculates the combinations of frequent movies and Case2 calculates the combinations of frequent users.

<b>Environment requirements</b>-
I have used Python 2.7 and Spark 2.2.1 to complete this assignment

<b>Command to execute the program</b>-
>$SPARK_HOME/bin/spark-submit Nupur_Shukla_SON.py case_number input_csv_file support

Example: $SPARK_HOME/bin/spark-submit Nupur_Shukla_SON.py 1 Data/Small1.csv 4

This command will generate an output file in the directory from where the command is run. The format of the output file name will be “Nupur_Shukla_SON_<Filename>.case<case>-<support>.txt”. For instance, for the above command, output file name will be -
Nupur_Shukla_SON_Small1.case1-4.txt

Note: Case number can only be either 1 or 2, for any other cases the program won’t run as expected.<br/>
Note: Paths can be relative to current directory or absolute.<br/>
Note: There should NOT be any spaces in the file path or file name<br/>

<b>Approach used to implement the program</b>-
In this program I have implemented 2 algorithms – SON and Apriori. According to SON algorithm, the input baskets gets partitioned into chunks. In phase 1, for each chunk I have implemented Apriori algorithm to find (local) frequent itemsets of all sizes. This is done by generating singletons candidates (C1), filtering them to form frequent singletons (L1) and then generating candidate pairs from L1, filtering them to form frequent pairs (L2) and then C3 -> L3 -> C4 and so on...until there are no more candidate itemsets. 
In the second phase of SON, again the input baskets get partitioned into chunks, and in each chunk, I count the occurrences of all of the candidate frequent itemsets (of all sizes) found in Phase 1. The reduce phase adds up all the counts, and filters out only the ones that are equal to or above the support threshold, thus giving out (global) frequent itemsets.

<b>Dataset 1</b><br/>
Case1 
>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 1 Data/Small2.csv 3 <br/>

Output file: Nupur_Shukla_SON_Small2.case1-3.txt

Case2
>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 2 Data/Small2.csv 5 <br/>

Output file: Nupur_Shukla_SON_Small2.case2-5.txt

<b>Dataset 2</b><br/>
The dataset can be downloaded from https://grouplens.org/datasets/movielens/ : ml-latest-small.zip <br/>
Case1
>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 1 ml-latest-small/ratings.csv 120 <br/>

Output file: Nupur_Shukla_SON_MovieLens.Small.case1-120.txt <br/>
<u>Execution Time </u>: 6 sec

>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 1 ml-latest-small/ratings.csv 150 <br/>

Output file: Nupur_Shukla_SON_MovieLens.Small.case1-150.txt <br/>
<u>Execution Time </u>: 5 sec

Case2
>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 2 ml-latest-small/ratings.csv 180 <br/>

Output file: Nupur_Shukla_SON_MovieLens.Small.case2-180.txt <br/>
<u>Execution Time </u>: 24 sec

>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 2 ml-latest-small/ratings.csv 200 <br/>

Output file: Nupur_Shukla_SON_MovieLens.Small.case2-200.txt <br/>
<u>Execution Time </u>: 19 sec


<b>Dataset 3</b>
The dataset can be downloaded from https://grouplens.org/datasets/movielens/ : ml-20m.zip <br/>
Case1
>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 1 ml-20m/ratings.csv 30000 <br/>

Output file: Nupur_Shukla_SON_MovieLens.Big.case1-30000.txt <br/>
<u>Execution Time </u>: 114 sec

>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 1 ml-20m/ratings.csv 35000 <br/>

Output file: Nupur_Shukla_SON_MovieLens.Big.case1-35000.txt <br/>
<u>Execution Time </u>: 85 sec

Case2
>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 2 ml-20m/ratings.csv 2800 <br/>

Output file: Nupur_Shukla_SON_MovieLens.Big.case2-2800.txt <br/>
<u>Execution Time </u>: 75 sec

>$SPARK_HOME/bin/spark-submit Solution/Nupur_Shukla_SON.py 2 ml-20m/ratings.csv 3000 <br/>

Output file: Nupur_Shukla_SON_MovieLens.Big.case2-3000.txt <br/>
<u>Execution Time </u>: 73 sec
