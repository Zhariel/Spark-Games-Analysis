package games
import org.apache.spark.sql.DataFrame

import java.nio.file.Paths

class TwitchData extends GameData {
  override val path: String = Paths.get("data", "twitch_games").toString

  override def clean_content(frame: DataFrame): DataFrame ={
    frame
  }

}
