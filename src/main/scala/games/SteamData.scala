package games
import case_classes.SteamGame
import org.apache.spark.sql.DataFrame

import java.nio.file.Paths


class SteamData extends GameData {
  override val path: String = Paths.get("data", "steam_games").toString

  override def clean_content(frame: DataFrame): DataFrame ={
    frame
  }

}
