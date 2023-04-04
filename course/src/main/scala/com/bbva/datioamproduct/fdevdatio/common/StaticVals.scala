package com.bbva.datioamproduct.fdevdatio.common

case object StaticVals {

  case object CourseConfigConstants {
    val RootConfig: String = "courseJob"
    val InputTag: String = s"$RootConfig.input"
    val OutputTag: String = s"$RootConfig.output"
    val Fifa22Tag: String = s"$OutputTag.fdevFifa22"
    val EjercicioFormTag: String = s"$OutputTag.ejercicioForm"


    val ClubPlayersTag: String = "fdevClubPlayers"
    val PlayersTag: String = "fdevPlayers"
    val NationalitiesTag: String = "fdevNationalities"
    val NationalPlayersTag: String = "fdevNationalPlayers"
    val ClubTeamsTag: String = "fdevClubTeams"
    val NationalTeamsTag: String = "fdevNationalTeams"
    val NationalTeamsErrorTag: String = "fdevNationalTeamsError"
  }

  case object EnvironmentVariables {
    private val RootParams: String = s"${CourseConfigConstants.RootConfig}.params"
    val NationalityIdTag: String = s"$RootParams.nationalityId"
    val CutoffDateTag: String = s"$RootParams.cutoffDate"
    val LeagueNameTag:String = s"$RootParams.leagueName"
  }

  case object Joins {
    val LeftAntiJoin: String = "left_anti"
    val LeftJoin: String = "left"
  }


}

