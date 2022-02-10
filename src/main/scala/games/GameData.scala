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
    val dff = spark.sql("SELECT * FROM table_steam LIMIT 20")
    spark.catalog.dropTempView("table_steam")
    dff
  }

  def ratio_viewer(df_steam: DataFrame, df_twitch: DataFrame): DataFrame  = {
    df_steam.createTempView("steam_table")
    df_twitch.createTempView("twitch_table")
    val df = spark.sql("SELECT month, (s.avg_players/t.viewers_average) FROM steam_table s, twitch_table t")
    spark.catalog.dropTempView("steam_table")
    spark.catalog.dropTempView("twitch_table")
    df
  }

  def more_players (df_steam: DataFrame): DataFrame={
    spark.catalog.dropTempView("steam_table")
    df_steam.createTempView("steam_table")
    val df = spark.sql("Select  month, avg_players from steam_table order by avg_players LIMIT 1")
    spark.catalog.dropTempView("steam_table")
    df
  }

  def nouveaux_streams(df_twitch: DataFrame): DataFrame = {
    df_twitch.createTempView("twitch_table")
    val df = spark.sql("SELECT SUM(viewers_gain) FROM twitch_table WHERE ((viewers_month LIKE '%2019%') OR (viewers_month LIKE '%2020%') OR (viewers_month LIKE '%2021%') OR (viewers_month LIKE '%Last%'))")
    spark.catalog.dropTempView("twitch_table")
    df
  }

  def plus_gros_peak(df_steam: DataFrame, df_twitch: DataFrame) : DataFrame =  {
    df_steam.createTempView("steam_table")
    df_twitch.createTempView("twitch_table")
    val df = spark.sql("SELECT t.viewers_peak, s.peak_players FROM steam_table s, twitch_table t WHERE ((s.month LIKE '%January 2022%') AND (t.viewers_month LIKE '%01/2022%'))")
    spark.catalog.dropTempView("steam_table")
    spark.catalog.dropTempView("twitch_table")
    df
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
