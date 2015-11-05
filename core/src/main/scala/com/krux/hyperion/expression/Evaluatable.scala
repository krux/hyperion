package com.krux.hyperion.expression

/**
 * Expressions that can be evaluated by Hyperion at runtime (create and deploy) the pipeline.
 */
trait Evaluatable[+T] {

  def evaluate(): T

}
