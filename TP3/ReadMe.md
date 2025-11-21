

# 💻 TP1: Programmation des RDDs avec Spark

## 🌟 Introduction

Ce travail pratique (*Travaux Pratiques* - TP) a pour objectif de maîtriser les fondamentaux de la programmation sur **Apache Spark** en utilisant les **Resilient Distributed Datasets (RDDs)**, ainsi que l'utilisation de **Spark SQL** pour l'analyse de données. Les exercices sont réalisés en **Java**.

## 🚀 Prérequis

  * **Java Development Kit (JDK):** Version 8 ou supérieure (version 17 recommandée pour les versions récentes de Spark).
  * **Apache Maven:** Outil de gestion de dépendances et de construction.
  * **Apache Spark:** Le projet est configuré pour utiliser la version **3.5.1** de Spark Core et Spark SQL.

### Dépendances Maven (`pom.xml`)

Le projet utilise les dépendances suivantes :

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.12</artifactId>
        <version>3.5.1</version>
    </dependency>

    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.12</artifactId>
        <version>3.5.1</version>
    </dependency>

    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>2.0.9</version>
    </dependency>
</dependencies>
```

## 📂 Structure des Fichiers

| Fichier | Description |
| :--- | :--- |
| `src/main/java/org/example/App1TotalVentesParVille.java` | Exercice 1.1 : Calcul du total des ventes par ville (RDD). |
| `src/main/java/org/example/App2TotalVentesVilleAnnee.java` | Exercice 1.2 : Calcul du total des ventes par ville et par année (Spark SQL/Dataset). |
| `src/main/java/org/example/WebLogAnalysisLocal.java` | Exercice 2 : Analyse de logs web (RDD). |
| `ventes.txt` | Fichier d'entrée pour l'Exercice 1 (ventes). |
| `data/access.log` | Fichier d'entrée pour l'Exercice 2 (logs web). |

-----

## 🎯 Exercice 1 : Programmation des RDDs - Analyse de Ventes

Cet exercice utilise le fichier d'entrée `ventes.txt`, dont la structure est : `date ville produit prix`.

### 1.1 Total des Ventes par Ville (RDD)

  * **Fichier :** `App1TotalVentesParVille.java`
  * **Objectif :** Déterminer le total des ventes pour chaque ville en utilisant les opérations **RDD** de base.
  * **Logique :**
    1.  Lire le fichier `ventes.txt` en tant que `JavaRDD<String>`.
    2.  Utiliser `mapToPair` pour transformer chaque ligne en un **PairRDD** de type `(ville, prix)`.
    3.  Utiliser `reduceByKey(Double::sum)` pour agréger les prix par ville.

### 1.2 Total des Ventes par Ville et par Année (Spark SQL/Dataset)

  * **Fichier :** `App2TotalVentesVilleAnnee.java`
  * **Objectif :** Calculer le prix total des ventes par ville et par année, en explorant l'approche **Dataset/DataFrame** et **Spark SQL**.
  * **Logique :**
    1.  Créer une session **SparkSession**.
    2.  Lire le fichier `ventes.txt` dans un **Dataset** (`df`), en spécifiant un **Schema** pour typer les colonnes.
    3.  Ajouter une colonne `annee` en extrayant l'année à partir de la colonne `date`.
    4.  Créer une vue temporaire (`ventes`).
    5.  Exécuter une requête **Spark SQL** avec `GROUP BY ville, annee` et `SUM(prix)`.

-----

## 🔒 Exercice 2 : Analyse de Fichiers de Logs avec RDD

Cet exercice utilise le fichier d'entrée `data/access.log`, respectant le format de log Apache.

  * **Fichier :** `WebLogAnalysisLocal.java`
  * **Objectif :** Effectuer une analyse statistique des requêtes du serveur web en utilisant les RDDs.

### Travail Réalisé

| Tâche | Description / Logique RDD |
| :--- | :--- |
| **1. Lecture des données** | Lecture de `data/access.log` dans un `JavaRDD<String>`. |
| **2. Extraction des champs** | Utilisation d'une **expression régulière simplifiée** (`LOG_PATTERN`) dans une fonction `parseLogLine` pour extraire l'**IP**, la **méthode**, la **ressource**, le **code HTTP** et la **taille** dans un objet `LogEntry`. Le RDD est filtré pour ne conserver que les entrées valides. |
| **3. Statistiques de base** | Calcul du **nombre total de requêtes** (`.count()`), du **nombre d'erreurs** (filtrage sur `httpCode >= 400`), et du **pourcentage d'erreurs**. |
| **4. Top 5 des adresses IP** | Utilisation de `mapToPair((ip, 1))` puis `reduceByKey(Integer::sum)` pour compter. Enfin, `mapToPair(Tuple2::swap)` et `sortByKey(false)` pour trier par décompte. |
| **5. Top 5 des ressources** | Logique similaire au Top 5 des IPs, en utilisant la ressource comme clé (`.getResource()`). |
| **6. Répartition des codes HTTP** | Comptage par code HTTP (`mapToPair((httpCode, 1))` et `reduceByKey(Integer::sum)`). |

-----

## ⚙️ Exécution

### Exécution en Local (IDE)

Pour exécuter les applications localement :

1.  Assurez-vous d'avoir les fichiers d'entrée (`ventes.txt` et `data/access.log`) aux emplacements corrects (à la racine du projet ou dans le dossier `data`).
2.  Exécutez la méthode `main` des classes correspondantes (`App1TotalVentesParVille`, `App2TotalVentesVilleAnnee`, `WebLogAnalysisLocal`) directement depuis votre IDE (IntelliJ, Eclipse, etc.).

<!-- end list -->

  * **Note pour `WebLogAnalysisLocal`:** Un chemin par défaut (`data/access.log`) est utilisé. La configuration `setMaster("local[*]")` permet une exécution en mode local sur tous les cœurs disponibles.
