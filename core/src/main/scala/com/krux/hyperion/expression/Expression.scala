package com.krux.hyperion.expression

/**
 * Expression. Expressions are delimited by: "#{" and "}" and the contents of the braces are
 * evaluated by AWS Data Pipeline.
 */
trait Expression {

  def content: String

  def toAws: String = s"#{$content}"

  override def toString: String = toAws

}

trait TypedExpression extends Expression

trait IntTypedExp extends TypedExpression { self =>

  def + (e: IntTypedExp) = new IntTypedExp {
    def content = s"${self.content} + ${e.content}"
  }

  def + (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} + ${e.content}"
  }

  def - (e: IntTypedExp) = new IntTypedExp {
    def content = s"${self.content} - ${e.content}"
  }

  def - (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} - ${e.content}"
  }

  def * (e: IntTypedExp) = new IntTypedExp {
    def content = s"${self.content} * ${e.content}"
  }

  def * (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} * ${e.content}"
  }

  def / (e: IntTypedExp) = new IntTypedExp {
    def content = s"${self.content} / ${e.content}"
  }

  def / (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} / ${e.content}"
  }

  /**
   * @note for ^ case, it always returns DoubleTypedExp regardless of the type of the paramenter
   */
  def ^ (e: IntTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} ^ ${e.content}"
  }

  def ^ (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} ^ ${e.content}"
  }

}

trait DoubleTypedExp extends TypedExpression { self =>

  def + (e: IntTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} + ${e.content}"
  }

  def + (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} + ${e.content}"
  }

  def - (e: IntTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} - ${e.content}"
  }

  def - (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} - ${e.content}"
  }

  def * (e: IntTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} * ${e.content}"
  }

  def * (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} * ${e.content}"
  }

  def / (e: IntTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} / ${e.content}"
  }

  def / (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} / ${e.content}"
  }

  def ^ (e: IntTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} ^ ${e.content}"
  }

  def ^ (e: DoubleTypedExp) = new DoubleTypedExp {
    def content = s"${self.content} ^ ${e.content}"
  }

}

trait StringTypedExp extends TypedExpression { self =>

  def + (e: StringTypedExp) = new StringTypedExp {
    def content = s"${self.content} + ${e.content}"
  }

}

trait BooleanTypedExp extends TypedExpression

trait DateTimeTypedExp extends TypedExpression {

  def + (period: Duration): DateTimeTypedExp = period match {
    case Minute(n) => PlusMinutes(this, IntExp(n))
    case Hour(n) => PlusHours(this, IntExp(n))
    case Day(n) => PlusDays(this, IntExp(n))
    case Week(n) => PlusWeeks(this, IntExp(n))
    case Month(n) => PlusMonths(this, IntExp(n))
    case Year(n) => PlusYears(this, IntExp(n))
  }

  def - (period: Duration): DateTimeTypedExp = period match {
    case Minute(n) => MinusMinutes(this, IntExp(n))
    case Hour(n) => MinusHours(this, IntExp(n))
    case Day(n) => MinusDays(this, IntExp(n))
    case Week(n) => MinusWeeks(this, IntExp(n))
    case Month(n) => MinusMonths(this, IntExp(n))
    case Year(n) => MinusYears(this, IntExp(n))
  }

  def year: IntTypedExp = ExtractYear(this)
  def month: IntTypedExp = ExtractMonth(this)
  def day: IntTypedExp = ExtractDay(this)
  def dayOfYear: IntTypedExp = DayOfYear(this)
  def hour: IntTypedExp = ExtractHour(this)
  def minute: IntTypedExp = ExtractMinute(this)

  def firstOfMonth: DateTimeTypedExp = FirstOfMonth(this)
  def midnight: DateTimeTypedExp = Midnight(this)
  def sunday: DateTimeTypedExp = Sunday(this)
  def yesterday: DateTimeTypedExp = Yesterday(this)
  def inTimeZone(zone: String): DateTimeTypedExp = InTimeZone(this, zone)

}

trait DurationTypedExp extends TypedExpression

trait S3UriTypedExp extends TypedExpression
