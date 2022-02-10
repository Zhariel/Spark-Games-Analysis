package games
import case_classes.TwitchWatch

import java.nio.file.Paths

class TwitchData extends GameData {
  override val path: String = Paths.get("data", "twitch_games").toString

}
