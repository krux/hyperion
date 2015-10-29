package com.krux.hyperion.common

trait OptionalOrdered[A] {

  /** Result of comparing `this` with operand `that`.
   *
   * Implement this method to determine how instances of A will be sorted.
   *
   * Returns Option(`x`) where:
   *
   *   - `Some(x < 0)` when `this < that`
   *
   *   - `Some(x == 0)` when `this == that`
   *
   *   - `Some(x > 0)` when  `this > that`
   *
   *   - `None` when `this` is not comparable to `that`
   *
   */
  def compare(that: A): Option[Int]

  /**
   * Returns Some(true) if `this` is less than `that`, or None if they are not comparable.
   */
  def < (that: A): Option[Boolean] = (this compare that).map(_ <  0)

  /**
   * Returns Some(true) if `this` is greater than `that`, or None if they are not comparable.
   */
  def > (that: A): Option[Boolean] = (this compare that).map(_ >  0)

  /**
   * Returns Some(true) if `this` is less than or equal to `that`, or None if they are not
   * comparable.
   */
  def <= (that: A): Option[Boolean] = (this compare that).map(_ <= 0)

  /**
   * Returns Some(true) if `this` is greater than or equal to `that`, or None if they are not
   * comparable.
   */
  def >= (that: A): Option[Boolean] = (this compare that).map(_ >= 0)

}
