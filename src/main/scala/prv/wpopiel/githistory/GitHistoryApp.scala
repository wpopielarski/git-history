package prv.wpopiel.githistory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import scala.annotation.meta.field
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ substring => subs, _ }

object GitHistoryApp extends App {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("git-history")
    .getOrCreate()

  try
    GitHistory.run(spark)
  finally
    if (spark ne null)
      spark.stop()
}

/**
 * Result of the job should output two aggregates saved as two separate files, one with repository
 *aggregates second with user aggregates.
 * 1. Repository aggregate fields
 * ○ Daily Date
 * ○ Project Id
 * ○ Project Name
 * ○ Number of unique users that starred it
 * ○ Number of unique users that forked it
 * ○ Number of created issues
 * ○ Number of created PRs
 * 2. User aggregate fields
 * ○ Daily Date
 * ○ User Id
 * ○ User Login
 * ○ Number of starred projects
 * ○ Number of created issues
 * ○ Number of created PRs
 */
object GitHistory {
  def pathFinder(struct: StructType, needField: String, path: String): List[String] = {
    struct.fields.foldLeft(List.empty[String]) { (acc, field) =>
      field match {
        case StructField(`needField`, StructType(_), _, _) =>
          val _path = if (path.isEmpty()) field.name else path + "." + field.name
          acc :+ _path
        case StructField(name, typ @ StructType(_), _, _) =>
          val _path = if (path.isEmpty()) name else path + "." + name
          acc ++ pathFinder(typ, needField, _path)
        case _ => acc
      }
    }
  }

  def allDataframes(df: Dataset[Row], paths: Seq[String]): Dataset[Row] = {
    paths match {
      case head :: tail => 
        val zero = df.select(col(head))
        tail.foldLeft(zero) { (acc, path) => acc.union(df.select(col(path))) }
      case _ =>
        df
    }
  }

  def getDates(history: Dataset[Row]): Seq[String] = {
    history
    .select(col("date"))
    .distinct()
    .collect()
    .map(_.getAs[String]("date"))
    .toList
  }

  def runSingle(history: Dataset[Row], date: String, userPaths: Seq[String], repoPaths: Seq[String]): Unit = {
    val spark = history.sparkSession
    import spark.implicits._
    val payload = history
                  .where($"payload".isNotNull)
                  .select($"payload")
    val users = allDataframes(history, userPaths)
                .alias("user")
                .where($"user".isNotNull)
                .select($"user.id", $"user.login")
                .distinct()
    val repos = allDataframes(history, repoPaths)
                .alias("repo")
                .where($"repo".isNotNull)
                .select($"repo.id", $"repo.name", $"repo.url", $"repo.forks_count", $"repo.stargazers_count")
                .groupBy($"id", $"name", $"url")
                .agg(max($"forks_count").as("forks_count"), max($"stargazers_count").as("stargazers_count"))
    val pulls = payload
                .alias("payload")
                .select($"payload.pull_request".alias("pr"))
                .select($"pr.id", $"pr.base.repo.id".alias("repo"), $"pr.user.id".alias("user"))
    val usersPulls = pulls
                     .groupBy($"user")
                     .agg(count("*").alias("user_pulls"))
    val repoPulls = pulls
                    .groupBy($"repo")
                    .agg(count("*").alias("repo_pulls"))
    val usersWithPulls = users
                         .join(usersPulls.withColumnRenamed("user", "id"), Seq("id"), "left")
                         .na.fill(0, Seq("user_pulls"))
    val repoWithPulls = repos
                        .join(repoPulls.withColumnRenamed("repo", "id"), Seq("id"), "left")
                        .na.fill(0, Seq("repo_pulls"))
    usersWithPulls.write.json(s"data/${date}-users.json")
    repoWithPulls.write.json(s"data/${date}-repos.json")
  }

  def run(spark: SparkSession,
          inputFile: Option[String] = None,
          outputReposFile: Option[String] = None,
          outputUsersFile: Option[String] = None): Unit = {
    import spark.implicits._
    
    val history = spark
                  .read
                  .json("data/*.json.gz")
                  .withColumn("date", subs(col("created_at"), 0, 10))
                  .drop($"created_at")
                  .cache()
    val payload = history
                  .where($"payload".isNotNull)
                  .select($"payload")
    val superSchema = payload.schema
    val userObjectNames = Seq("user", "owner", "merged_by")
    val userPaths = userObjectNames.flatMap { needField =>
      pathFinder(superSchema, needField, "")
    }
    val repoObjectNames = Seq("repo")
    val repoPaths = repoObjectNames.flatMap { needField =>
      pathFinder(superSchema, needField, "")
    }
    val dates = getDates(history)
    dates.foreach(date => runSingle(history.where($"date" === date), date, userPaths, repoPaths))
  }
}
