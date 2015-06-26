/*
 * Copyright 2015 Philip L. McMahon
 *
 * Philip L. McMahon licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package rascql.postgresql.stream

import akka.stream._
import akka.stream.scaladsl._

/**
 * Partitions elements based on a predicate when demand is available for both
 * [[Outlet]]s.
 *
 * @author Philip L. McMahon
 */
class Partition[T](fn: T => Boolean)
  extends FlexiRoute[T, PartitionShape[T]](
    new PartitionShape[T], Attributes.name("Partition")) {

  import FlexiRoute._

  override def createRouteLogic(p: PortT) = new RouteLogic[T] {

    def initialState = State[Any](DemandFromAll(p.outlets)) {
      (ctx, _, elem) =>
        ctx.emit(if (fn(elem)) p.matched else p.unmatched)(elem)
        SameState
    }

    override def initialCompletionHandling = eagerClose

  }

}

object Partition {

  def apply[T](fn: T => Boolean): Partition[T] = new Partition(fn)

}

import FanOutShape._

class PartitionShape[T] private[stream] (_init: Init[T] = Name[T]("Partition"))
  extends FanOutShape[T](_init) {

  val matched = newOutlet[T]("matched")
  val unmatched = newOutlet[T]("unmatched")

  protected override def construct(i: Init[T]) = new PartitionShape[T](i)

}
