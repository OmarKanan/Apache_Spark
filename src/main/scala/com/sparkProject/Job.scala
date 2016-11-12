package com.sparkProject

// Import des modules nécessaires
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.tuning.{TrainValidationSplit, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


object Job {

  // Point d'entrée du programme
  def main(args: Array[String]): Unit = {

    // Création d'une session Spark
    val spark = SparkSession
          .builder
          .master("local")
          .appName("spark session TP_parisTech")
          .getOrCreate()
          
    import spark.implicits._
     
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Phase de Preprocessing
    
    // Chargement du fichier csv dans un DataFrame    
    val df = spark.read
      .option("header", "true")
      .option("comment", "#")
      .option("inferSchema", "true")
      .csv("files/cumulative.csv")
    
    // On enlève les lignes dont le label vaut "CANDIDATE" car on veut construire un modèle binaire
    // On enlève également les colonnes non utiles pour la prédiction
    val df_filtered = df
      .filter("koi_disposition != 'CANDIDATE'")
      .drop("koi_eccen_err1", "index","kepid", "koi_fpflag_nt", "koi_fpflag_ss", "koi_fpflag_co", 
          "koi_fpflag_ec", "koi_sparprov", "koi_trans_mod", "koi_datalink_dvr", "koi_datalink_dvs", 
          "koi_tce_delivname", "koi_parm_prov", "koi_limbdark_mod", "koi_fittype", "koi_disp_prov", 
          "koi_comment", "kepoi_name", "kepler_name", "koi_vet_date", "koi_pdisposition")
      
    // On détecte les colonnes constantes (leur "countDistinct" vaut au plus 1)
    val useless_columns = df_filtered.columns.filter(
        (column: String) => df_filtered.agg(countDistinct(column)).first().getLong(0) <= 1)
    
    // On enlève ces colonnes car elles sont inutiles, puis on remplace les valeurs manquantes par des 0
    val df_clean = df_filtered
      .drop(useless_columns:_*)
      .na.fill(0)
    
    // On fait la jointure demandée (dont je n'ai pas compris l'intérêt car elle retourne le même
    // DataFrame df_clean ?)
    // Puis on crée les 2 colonnes "koi_ror_min" et "koi_ror_max"
    val df_final = df_clean
      .drop("koi_disposition")
      .join(df_clean.select("rowid", "koi_disposition"), usingColumn="rowid")
      .withColumn("koi_ror_min", $"koi_ror_err2" + $"koi_ror")
      .withColumn("koi_ror_max", $"koi_ror_err1" + $"koi_ror")
    
    // On enregistre sur disque le DataFrame obtenu
    df_final
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("files/cleanCumulative")
             
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Phase de Machine Learning
      
    // Objet qui va assemblera les features sous forme de "Vector"
    val vecAssembler = new VectorAssembler()
      .setInputCols(df_final.columns diff Array("koi_disposition"))
      .setOutputCol("features")
    
    // Objet qui transformera la colonne à prédire en colonne de 0 et de 1 pour la classification
    val labelIndexer = new StringIndexer()
      .setInputCol("koi_disposition")
      .setOutputCol("label")
    
    // Objet qui effectuera la régression logistique, avec standardisation des features
    val logisticReg = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setStandardization(true)
      .setFitIntercept(true)
      .setTol(1.0e-5)
      .setMaxIter(300)
      .setElasticNetParam(1.0)
    
    // Pipeline qui chaînera les 3 objets précédents
    val pipeline = new Pipeline()
      .setStages(Array(vecAssembler, labelIndexer, logisticReg))
    
    // Evaluateur binaire qui calculera la précision des prédictions
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
    
    // Grille des différentes valeurs de l'hyper-paramètre de la régression logistique
    val regParamGrid = new ParamGridBuilder()
      .addGrid(logisticReg.regParam, 
          (Array(-6, -5, -4, -3, -2, -1, 0).map((pow: Int) => Math.pow(10, pow)) 
          ++ Array(-6, -5, -4, -3, -2, -1).map((pow: Int) => 5 * Math.pow(10, pow))))
      .build()
    
    // Objet final qui va, pour chaque valeur de la grille précédente, appliquer le pipeline sur
    // 70% des données qui lui seront transmises et appliquer l'évaluateur sur les 30% de données 
    // restantes, et enfin sélectionner le modèle ayant obtenu la meilleure précision
    val splitValidation = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(regParamGrid)
      .setTrainRatio(0.7)
    
    // On garde 90% des données pour créer le modèle, et 10% pour le tester
    val Array(trainSet, testSet) = df_final.randomSplit(Array(0.9, 0.1), seed=4537)
    
    // On calcule le modèle sur les 90% de données grâce à l'objet "splitValidation" précédemment créé
    val bestModel = splitValidation.fit(trainSet)
    
    // On applique le modèle sur les 10% de données de test
    val predictions = bestModel.transform(testSet)
    
    // On affiche les résultats
    predictions.groupBy("label", "prediction").count.show()
    print("Accuracy = " + evaluator.evaluate(predictions))
    
    // Pour finir on enregistre le modèle sur disque
    bestModel.write.overwrite().save("files/model")
    
  }

}
