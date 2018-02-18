package com.krux.hyperion.resource

trait EmrApplication {
  def serialize: String
}

object EmrApplication {

  case object Flink extends EmrApplication {
    val serialize = "Flink"
  }

  case object Ganglia extends EmrApplication {
    val serialize = "Ganglia"
  }

  case object HBase extends EmrApplication {
    val serialize = "HBase"
  }

  case object HCatalog extends EmrApplication {
    val serialize = "HCatalog"
  }

  case object Hive extends EmrApplication {
    val serialize = "Hive"
  }

  case object Hue extends EmrApplication {
    val serialize = "Hue"
  }

  case object Livy extends EmrApplication {
    val serialize = "Livy"
  }

  case object Mahout extends EmrApplication {
    val serialize = "Mahout"
  }

  case object MXNet extends EmrApplication {
    val serialize = "MXNet"
  }

  case object Oozie extends EmrApplication {
    val serialize = "Oozie"
  }

  case object Phoenix extends EmrApplication {
    val serialize = "Phoenix"
  }

  case object Pig extends EmrApplication {
    val serialize = "Pig"
  }

  case object Presto extends EmrApplication {
    val serialize = "Presto"
  }

  case object Spark extends EmrApplication {
    val serialize = "Spark"
  }

  case object Sqoop extends EmrApplication {
    val serialize = "Sqoop"
  }

  case object Tez extends EmrApplication {
    val serialize = "Tez"
  }

  case object Zeppelin extends EmrApplication {
    val serialize = "Zeppelin"
  }

  case object ZooKeeper extends EmrApplication {
    val serialize = "ZooKeeper"
  }

}
