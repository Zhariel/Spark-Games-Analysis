import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SteamData extends App {
  val spark = SparkSession
    .builder()
    .appName(name = "Playtime Viewership Evaluation")
    .master("local[*]")
    .getOrCreate()


  // About Steam

  def get_file_to_df(path: String) : DataFrame = {
    return spark.read.option("header",true).csv(path)
  }

  val df_amongus_steam        = get_file_to_df("data/steam_games/AmongUs.csv")
  val df_border_steam         = get_file_to_df("data/steam_games/Borderlands2.csv")
  val df_counter_steam        = get_file_to_df("data/steam_games/CounterStrikeGlobalOffensive.csv")
  val df_cyber_steam          = get_file_to_df("data/steam_games/Cyberpunk2077.csv")
  val df_dontstarve_steam     = get_file_to_df("data/steam_games/DontStarveTogether.csv")
  val df_dota_steam           = get_file_to_df("data/steam_games/Dota2.csv")
  val df_grand_steam          = get_file_to_df("data/steam_games/GrandTheftAutoV.csv")
  val df_pubg_steam           = get_file_to_df("data/steam_games/PUBGBATTLEGROUNDS.csv")
  val df_rust_steam           = get_file_to_df("data/steam_games/Rust.csv")
  val df_team_steam           = get_file_to_df("data/steam_games/TeamFortress2.csv")
  val df_terraria_steam       = get_file_to_df("data/steam_games/Terraria.csv")

  case class steam_game(Month: String, Avg_Players : String, Gain : String, pourcentage_gain : String, peak_players : String)
  val encoder_steam           = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[steam_game]

  val ds_amongus_steam : Dataset[steam_game] = df_amongus_steam.as(encoder_steam)
  val ds_border_steam : Dataset[steam_game] = df_border_steam.as(encoder_steam)
  val ds_counter_steam : Dataset[steam_game] = df_counter_steam.as(encoder_steam)
  val ds_cyber_steam : Dataset[steam_game] = df_cyber_steam.as(encoder_steam)
  val ds_dontstarve_steam : Dataset[steam_game] = df_dontstarve_steam.as(encoder_steam)
  val ds_dota_steam : Dataset[steam_game] = df_dota_steam.as(encoder_steam)
  val ds_grand_steam : Dataset[steam_game] = df_grand_steam.as(encoder_steam)
  val ds_pubg_steam : Dataset[steam_game] = df_pubg_steam.as(encoder_steam)
  val ds_team_steam : Dataset[steam_game] = df_team_steam.as(encoder_steam)
  val ds_terraria_steam : Dataset[steam_game] = df_terraria_steam.as(encoder_steam)


  //About Twitch
  case class twitch_watch(viewers_Month: String, viewers_Average: String, viewers_Gain: String, viewers_poucentage_Gain: String, viewers_Peak: String, streams_Month : String, streams_Average: String,
                          streams_Gain: String, streams_Pourcentage_Gain: String, streams_Peak: String, watched_Month: String, watched_Value : String, watched_Gain : String, watched_Pourcentage_Gain : String)
  val encoder_twitch           = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[twitch_watch]

  val df_amongus_twitch        = get_file_to_df("data/twitch_games/AmongUs.csv")
  val df_border_twitch         = get_file_to_df("data/twitch_games/Borderlands2.csv")
  val df_counter_twitch        = get_file_to_df("data/twitch_games/CounterStrikeGlobalOffensive.csv")
  val df_cyber_twitch          = get_file_to_df("data/twitch_games/Cyberpunk2077.csv")
  val df_dontstarve_twitch     = get_file_to_df("data/twitch_games/DontStarveTogether.csv")
  val df_dota_twitch           = get_file_to_df("data/twitch_games/Dota2.csv")
  val df_grand_twitch          = get_file_to_df("data/twitch_games/GrandTheftAutoV.csv")
  val df_pubg_twitch           = get_file_to_df("data/twitch_games/PUBGBATTLEGROUNDS.csv")
  val df_rust_twitch           = get_file_to_df("data/twitch_games/Rust.csv")
  val df_team_twitch           = get_file_to_df("data/twitch_games/TeamFortress2.csv")
  val df_terraria_twitch       = get_file_to_df("data/twitch_games/Terraria.csv")

  val ds_amongus_twitch : Dataset[twitch_watch] = df_amongus_twitch.as(encoder_twitch)
  val ds_border_twitch : Dataset[twitch_watch] = df_border_twitch.as(encoder_twitch)
  val ds_counter_twitch : Dataset[twitch_watch] = df_counter_twitch.as(encoder_twitch)
  val ds_cyber_twitch : Dataset[twitch_watch] = df_cyber_twitch.as(encoder_twitch)
  val ds_dontstarve_twitch : Dataset[twitch_watch] = df_dontstarve_twitch.as(encoder_twitch)
  val ds_dota_twitch : Dataset[twitch_watch] = df_dota_twitch.as(encoder_twitch)
  val ds_grand_twitch : Dataset[twitch_watch] = df_grand_twitch.as(encoder_twitch)
  val ds_pubg_twitch : Dataset[twitch_watch] = df_pubg_twitch.as(encoder_twitch)
  val ds_team_twitch : Dataset[twitch_watch] = df_team_twitch.as(encoder_twitch)
  val ds_terraria_twitch : Dataset[twitch_watch] = df_terraria_twitch.as(encoder_twitch)

  ds_terraria_twitch.show()
}
