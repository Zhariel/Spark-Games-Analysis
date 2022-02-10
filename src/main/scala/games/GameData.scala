package games
import games.DataUtils
import org.apache.spark.sql.DataFrame

import java.io.File
import java.nio.file.Paths

abstract class GameData() {
  val utils = new DataUtils
  val spark = utils.spark
  val path = ""

  def extract_dataframes_in_folder(): Map[String, DataFrame] ={
    val utils = new DataUtils()
    val list_of_games = utils.getListOfFiles(path)

    val games_names_list = list_of_games.map(x => x.getName.substring(0, x.getName.length-4))
    val df_list = list_of_games.map(x => get_file_to_df(x.getPath))

    (games_names_list zip df_list).toMap
  }

  private def get_file_to_df(path: String): DataFrame = {
     spark.read
    .format("csv")
    .option("header",value = true)
    .option("sep", ",")
    .load(path)
  }
}
