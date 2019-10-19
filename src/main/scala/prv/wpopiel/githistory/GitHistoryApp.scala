package prv.wpopiel.githistory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import scala.annotation.meta.field
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ substring => subs, _ }
import org.apache.spark.sql.expressions.Window

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
  def makeName(path: String, name: String): String = {
    if (path.isEmpty()) name else path + "." + name
  }

  def pathFinder(struct: StructType, needField: String, path: String): List[String] = {
    struct.fields.foldLeft(List.empty[String]) { (acc, field) =>
      field match {
        case StructField(`needField`, StructType(_), _, _) =>
          acc :+ makeName(path, needField)
        case StructField(name, typ @ StructType(_), _, _) =>
          acc ++ pathFinder(typ, needField, makeName(path, name))
        case _ => acc
      }
    }
  }

  def allDataframes(df: Dataset[Row], paths: Seq[String]): Dataset[Row] = {
    paths match {
      case head :: tail => 
        val zero = df.select(col(head), col("created_at"))
        tail.foldLeft(zero) { (acc, path) => acc.union(df.select(col(path), col("created_at"))) }
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

  object repo {
    def apply(root: Dataset[Row], paths: Seq[String]): Dataset[Row] = {
      val spark = root.sparkSession
      import spark.implicits._
      val byDate = Window
                   .partitionBy($"id")
                   .orderBy($"created_at".desc)
      val repo = allDataframes(root, paths)
                 .alias("repo")
                 .where($"repo".isNotNull)
                 .select($"created_at", $"repo.id", $"repo.name", $"repo.url", $"repo.forks_count", $"repo.stargazers_count", $"repo.open_issues_count")
      repo
      .withColumn("last", row_number.over(byDate))
      .where($"last" === 1)
      .drop($"last")
    }
  }

  object user {
    def apply(root: Dataset[Row], paths: Seq[String]): Dataset[Row] = {
      val spark = root.sparkSession
      import spark.implicits._
      allDataframes(root, paths)
      .alias("user")
      .where($"user".isNotNull)
      .select($"user.id", $"user.login")
      .distinct()
    }
  }

  def runSingle(history: Dataset[Row], date: String, userPaths: Seq[String], repoPaths: Seq[String]): Unit = {
    val spark = history.sparkSession
    import spark.implicits._
    val payload = history
                  .where($"payload".isNotNull)
                  .select($"payload")
    val repos = repo(history, repoPaths)
    val users = user(history, userPaths)
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
    val usersWithPulls = broadcast(users)
                         .join(usersPulls.withColumnRenamed("user", "id"), Seq("id"), "left")
                         .na.fill(0, Seq("user_pulls"))
    val repoWithPulls = broadcast(repos)
                        .join(repoPulls.withColumnRenamed("repo", "id"), Seq("id"), "left")
                        .na.fill(0, Seq("repo_pulls"))
    val issues = payload
                 .alias("payload")
                 .select($"payload.issue").as("issue")
                 .where($"issue".isNotNull)
                 .select($"issue.id".as("id"), $"issue.user.id".as("user"))
                 .groupBy($"user")
                 .agg(count("*").alias("user_issues_count"))
    val usersWithPullsAndIssues = broadcast(usersWithPulls)
                                  .join(issues.withColumnRenamed("user", "id"), Seq("id"), "left")
                                  .na.fill(0, Seq("user_issues_count"))
    usersWithPullsAndIssues.coalesce(numPartitions = 1).write.json(s"data/${date}-users.json")
    repoWithPulls.coalesce(numPartitions = 1).write.json(s"data/${date}-repos.json")
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
                  .cache()
    val superSchema = history
                      .where($"payload".isNotNull)
                      .select($"payload")
                      .schema
    val pathsFindInSchema = (objNames: Seq[String]) => objNames.flatMap { needField =>
      pathFinder(superSchema, needField, "")
    }
    val userObjectNames = Seq("user", "owner", "merged_by")
    val repoObjectNames = Seq("repo")
    val dates = getDates(history)
    dates.foreach { date =>
      runSingle(history.where($"date" === date), date, pathsFindInSchema(userObjectNames), pathsFindInSchema(repoObjectNames))
    }
  }
}
