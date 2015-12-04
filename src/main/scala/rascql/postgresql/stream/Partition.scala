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
import akka.stream.stage._

/**
 * Partitions elements based on a predicate when demand is available for both
 * [[Outlet]]s.
 *
 * @author Philip L. McMahon
 */
class Partition[T](predicate: T => Boolean)
  extends GraphStage[PartitionShape[T]] {

  val shape = new PartitionShape[T]

  def createLogic(attrs: Attributes) = new GraphStageLogic(shape) {

    setHandler(shape.in, new InHandler {
      def onPush() = {
        val e = grab(shape.in)
        emit(if (predicate(e)) shape.matched else shape.unmatched, e)
      }
    })

    val puller = new OutHandler {
      def onPull() = if (!hasBeenPulled(shape.in)) pull(shape.in)
    }

    shape.outlets.foreach(setHandler(_, puller))

  }

}

object Partition {

  def apply[T](p: T => Boolean): Partition[T] = new Partition(p)

}

import FanOutShape._

class PartitionShape[T] private[stream] (_init: Init[T] = Name[T]("Partition"))
  extends FanOutShape[T](_init) {

  val matched = newOutlet[T]("matched")
  val unmatched = newOutlet[T]("unmatched")

  protected override def construct(i: Init[T]) = new PartitionShape[T](i)

}
