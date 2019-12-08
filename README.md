# Spark project MS Big Data Télécom : Kickstarter campaigns

Spark project for MS Big Data Telecom based on Kickstarter campaigns 2019-2020

## Nettoyage et préparation des données :
nous avons effectué ce qui était demandé dans le TP, et extrait les features supplémentaires : heure de lancement (launch), jour de lancement (launch), mois de lancement () en espérant qu'il y ait des moments plus propices pour lancer une campagne.

## Modèle:
nous avons ajouté aux features les caractéristiques du lancement, et tenté dans la grille de paramètres de jouer sur d'autre paramètres de la régression logistique :

## Résultats :
TP initial:
sans gridsearch : 0.6294264707721853

|final_status|predictions|count|
|:----------:|:---------:|----:|
|           1|        0.0| 1724|
|           0|        1.0| 2303|
|           1|        1.0| 1634|
|           0|        0.0| 4999|

avec grid search : 0.6573114777717257

|final_status|predictions|count|
|:----------:|:---------:|----:|
|           1|        0.0| 1000|
|           0|        1.0| 2787|
|           1|        1.0| 2358|
|           0|        0.0| 4515|

En tenant compte de la période de lancement, et avec des paramètres additionels de gridsearch:

sans gridsearch : 0.6302840073390044

|final_status|predictions|count|
|:----------:|:---------:|----:|
|           1|        0.0| 1718|
|           0|        1.0| 2300|
|           1|        1.0| 1640|
|           0|        0.0| 5002|

avec grid search : 0.6580095263663649

|final_status|predictions|count|
|:----------:|:---------:|----:|
|           1|        0.0| 1012|
|           0|        1.0| 2767|
|           1|        1.0| 2346|
|           0|        0.0| 4535|


Le gain étant très minime (on passe de 0.6573 à 0.6580) nous n'avons pas de réelle amélioration.
