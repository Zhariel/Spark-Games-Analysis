package games
import case_classes.SteamGame

import java.nio.file.Paths


class SteamData extends GameData {
  override val path: String = Paths.get("data", "steam_games").toString

}
