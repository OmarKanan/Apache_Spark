# MS Big Data 2016/2017 - TP Spark - Omar KANAN

Pour exécuter le programme, pas besoin de modifier les paths dans le code, il faut par contre se placer à la racine du projet lorsque l'on lance la commande spark-submit. Ma commande bash est la suivante :  

~~~
$ ~/Applications/spark-2.0.0-bin-hadoop2.6/bin/spark-submit 
--driver-memory 1G --executor-memory 1G --class com.sparkProject.Job --master spark://omar-Ubuntu:7077
~/Documents/Telecom_ParisTech/Eclipse/workspace/Scala/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar
~~~

**Remarques :**  
~~~
~/Applications/spark-2.0.0-bin-hadoop2.6/bin/spark-submit
~~~
est à remplacer par le path de votre commande spark-submit
~~~
spark://omar-Ubuntu:7077
~~~
est à remplacer par l'adresse de votre serveur spark  
~~~
~/Documents/Telecom_ParisTech/Eclipse/workspace/Scala/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar
~~~
est à remplacer par le path du fichier jar créé lorsque vous avez compilé le programme avec la commande "sbt assembly"  
