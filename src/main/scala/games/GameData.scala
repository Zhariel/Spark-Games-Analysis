package games
import games.DataUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.nio.file.Paths

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

  private def file_to_df(path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", value = true)
      .option("sep", ",")
      .load(path)
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
