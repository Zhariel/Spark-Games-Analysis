package games
import games.DataUtils

import java.io.File

abstract class GameData(var name: String) {
  val utils = new DataUtils

  def extract_dataframes_in_folder(folder_path: String): List[Any] ={
    val utils = new DataUtils()
    val list_of_games = utils.getListOfFiles(folder_path)

  }

  private def extract_file(path: String): Any = {
    val spark = utils.spark
    val dataframe = spark.read
      .format("csv")
      .option("sep", ",")
      .load(path)
  }
}
