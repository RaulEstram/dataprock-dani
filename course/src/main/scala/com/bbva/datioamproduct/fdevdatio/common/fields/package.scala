package com.bbva.datioamproduct.fdevdatio.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

package object fields {
  case object Age extends Field {
    override val name: String = "age"
  }

  case object Overall extends Field {
    override val name: String = "overall"
  }

  private case object Height extends Field {
    override val name: String = "height_cm"
  }

  case object SofifaId extends Field {
    override val name: String = "sofifa_id"
  }


  case object ClubTeamId extends Field {
    override val name: String = "club_team_id"
  }

  case object NationTeamId extends Field {
    override val name: String = "nation_team_id"
  }

  case object NationalityId extends Field {
    override val name: String = "nationality_id"
  }

  case object NationalityName extends Field {
    override val name: String = "nationality_name"
  }

  case object LeagueName extends Field {
    override val name: String = "league_name"
  }

  case object ClubName extends Field {
    override val name: String = "club_name"
  }


  case object ShortName extends Field {
    override val name: String = "short_name"
  }

  case object ClubPositions extends Field {
    override val name: String = "club_position"
  }

  case object CatAge extends Field {
    override val name: String = "cat_age_overall"

    def apply(): Column = {
      when(Age.column <= 20 || Overall.column > 80, "A")
        .when(Age.column <= 23 || Overall.column > 70, "B")
        .when(Age.column <= 30, "C")
        .otherwise("D")
        .alias(name)
    }
  }

  case object CatHeight extends Field {
    override val name: String = "cat_height_cm"

    def apply(): Column = {
      when(Height.column > 200, "A")
        .when(Height.column >= 185, "B")
        .when(Height.column > 175, "C")
        .when(Height.column >= 165, "D")
        .otherwise("E")
        .alias(name)
    }

  }


  case object CutoffDate extends Field {
    override val name: String = "cutoff_date"

    def apply(date: String): Column = {
      lit(date) alias name
    }
  }

  case object ShortNameLit extends Field {
    override val name: String = "short_name"

    def apply(): Column = lit("nuevo valor") alias name
  }


  case object PlayerPositions extends Field {
    override val name: String = "player_positions"

    def apply(): Column = split(column, ", ") alias name
  }

  case object PlayerTraits extends Field {
    override val name: String = "player_traits"

    def apply(): Column = split(trim(column), ",") alias name
  }

  case object OverallByNationality extends Field {
    override val name: String = "overall_by_nationality"

    def apply(): Column = avg(Overall.column) alias name
  }

  case object PlayersByNationality extends Field {
    override val name: String = "players_by_nationality"

    def apply(): Column = count(Overall.column) alias name
  }

  case object PlayerPosition extends Field {
    override val name: String = "player_positions"

    def apply(): Column = split(regexp_replace(column, " ", ""), ",") alias name
  }

  case object ExplodePlayerPositions extends Field {
    override val name: String = "explode_player_positions"

    def apply(): Column = explode(PlayerPosition.column) alias name
  }

  case object ExplodePlayerTraits extends Field {
    override val name: String = "player_traits_exploded"

    def apply(): Column = explode(PlayerTraits.column) alias name
  }

  case object CountByPlayerPositions extends Field {
    override val name: String = "count_by_position"

    def apply(): Column = count(ExplodePlayerPositions.column) alias name
  }

  case object OverallPlayersPositions extends Field {
    override val name: String = "Overall_avg"

    def apply(): Column = avg(Overall.column) alias name
  }


  /*
  WindowSpec
   */

  case object StdDevOverall extends Field {
    override val name: String = "std_dev_overall"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column, CatAge.column)
      stddev(Overall.column) over w alias name
    }
  }

  case object MeanOverall extends Field {
    override val name: String = "mean_overall"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column, CatAge.column)
      mean(Overall.column) over w alias name
    }
  }

  /*
    case object AgeRn extends Field {
      override val name: String = "age_rn"

      def apply(): Column = {
        val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
        row_number() over w alias name // es como constar la cantidad de registros que hay en el orden que aparecen
      }
    }

    case object AgeDenseRank extends Field {
      override val name: String = "dense_rank"

      def apply(): Column = {
        val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
        dense_rank() over w alias name // es como contar el top de los registros, y va 1 tras 1: 1 1 1 2 2 3 4
      }
    }

    case object AgeRank extends Field {
      override val name: String = "age_rank"

      def apply(): Column = {
        val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
        rank() over w alias name // es como contar el top de los registros, y va de 1 tra el siguiente numero de registro: 1 1 1 4 4 4 7
      }
    }

    case object ShortNameLead extends Field {
      override val name: String = "sort_name_lead"

      def apply(): Column = {
        val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
        lead(ShortName.column, 1) over w alias name // te trae el registro siguiente
      }
    }


  case object ShortNameLag extends Field {
    override val name: String = "sort_name_lag"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
      lag(ShortName.column, 1) over w alias name // te trae el registro anterior
    }
  }
  */

  case object ZScore extends Field {
    override val name: String = "z_Score"

    def apply(): Column = {
      ((Overall.column - MeanOverall()) / StdDevOverall()) alias name
    }
  }


  case object SumOverall extends Field {
    override val name: String = "sum_overall"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(ClubName.column, CatAge.column).orderBy(Overall.column)
      sum(Overall.column) over w alias name
    }
  }

}
