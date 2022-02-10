package games
import case_classes.{SteamGame, TwitchWatch}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

abstract class GameData() {
  val utils = new DataUtils
  val spark: SparkSession = utils.spark
  val path = ""

  private def extract_names(): List[String] = {
    utils.getListOfFiles(path).map(x => x.getName.substring(0, x.getName.length - 4))
  }

  def extract_dataframes_in_folder(): List[DataFrame] = {
    val list_of_games = utils.getListOfFiles(path)
    list_of_games.map(x => file_to_df(x.getPath))
  }

  def file_to_df(path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", value = true)
      .option("sep", ",")
      .load(path)
  }

  def get_df_steam_to_ds(df: DataFrame) : Dataset[SteamGame] = {
    df.as(ExpressionEncoder[SteamGame])
  }

  def get_df_twitch_to_ds(df: DataFrame) : Dataset[TwitchWatch] = {
    df.as(ExpressionEncoder[TwitchWatch])
  }

  def view_table(df: DataFrame) : DataFrame = {
    df.createTempView("table_steam")
    spark.sql("SELECT * FROM table_steam LIMIT 20")
  }

  def ratio_viewer(df_steam: DataFrame, df_twitch: DataFrame): DataFrame  = {
    df_steam.createTempView("steam_table")
    df_twitch.createTempView("twitch_table")
    spark.sql("SELECT Month, round((avg_players/viewers_average), 2) FROM steam_table, twitch_table")
  }

  def more_players (df_steam: DataFrame): DataFrame={
    //df_steam.withColumn("avg_players", df_steam["avg_players"].cast(DoubleType()).alias("avg_players"))
    df_steam.createTempView("steam_table")
    spark.sql("Select  month, CAST(avg_players, float) from steam_table order by CAST(avg_players, float) LIMIT 1")
  }

  def nouveaux_streams(df_twitch: DataFrame): DataFrame = {
    df_twitch.createTempView("twitch_table")
    spark.sql("SELECT SUM(viewers_gain) FROM twitch_table WHERE ((month LIKE '%2019%') OR (month LIKE '%2020%') OR (month LIKE '%2021%'))")
  }

  def clean_header(frame: DataFrame): DataFrame ={
    val frame2 = frame
      .columns
      .map(c => c.toLowerCase()
        .replace(".", "_")
        .replace(" ", "_")
        .replaceAll("%", "percent")
      )
    frame.toDF(frame2:_*)
  }

  def clean_content(frame: DataFrame): DataFrame

  def clean_dataframe(frame: DataFrame): DataFrame = {
    val frame_clean_headers = clean_header(frame)
    clean_content(frame_clean_headers)
  }
}
