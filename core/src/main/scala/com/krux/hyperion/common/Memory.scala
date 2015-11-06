package com.krux.hyperion.common

/**
 * Memory is a memory specification including an amount and a unit.
 *
 * @param n The amount of memory
 * @param unit The unit.
 */
case class Memory(n: Long, unit: String) {
  override val toString: String = s"$n$unit"
}
