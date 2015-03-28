package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpSqlActivity, AdpEc2Resource, AdpRef, AdpDatabase, AdpActivity}

case class SqlActivity (
    id: String,
    runsOn: Ec2Resource,
    database: Database,
    script: String,
    scriptArgument: Seq[String],
    dependsOn: Seq[PipelineActivity]
  ) extends PipelineActivity {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)

  def serialize = AdpSqlActivity(
      id = id,
      name = Some(id),
      database = AdpRef[AdpDatabase](database.id),
      script = script,
      scriptArgument = scriptArgument match {
        case Seq() => None
        case other => Some(other)
      },
      queue = None,
      dependsOn = dependsOn match {
        case Seq() => None
        case other => Some(other.map(a => AdpRef[AdpActivity](a.id)))
      },
      runsOn = AdpRef[AdpEc2Resource](runsOn.id)
    )

  override def objects: Iterable[PipelineObject] = runsOn +: dependsOn
}
