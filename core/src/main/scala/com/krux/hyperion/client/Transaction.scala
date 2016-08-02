package com.krux.hyperion.client


trait Transaction[F, S] {

  def action(): S

  def validate(result: S): Boolean

  def rollback(result: S): F

  def apply(): Either[F, S] = {
    val result = action()
    if (validate(result)) Right(result) else Left(rollback(result))
  }

}
