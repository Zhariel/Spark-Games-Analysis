package games
import case_classes.SteamGame
import org.apache.spark.sql.DataFrame

import java.nio.file.Paths
import scala.language.postfixOps


class SteamData extends GameData {
  override val path: String = Paths.get("data", "steam_games").toString

  val year = Map(
    "January" -> "01",
    "February" -> "02",
    "March " -> "03",
    "April" -> "04",
    "May" -> "05",
    "June" -> "06",
    "July" -> "07",
    "January" -> "08",
    "September" -> "09",
    "October" -> "10",
    "November" -> "11",
    "December" -> "12",
  )

  override def clean_content(frame: DataFrame): DataFrame = {
    frame
  }
}
