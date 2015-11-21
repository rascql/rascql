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
 * Creates an ordered sequence of [[Outlet]]s, which will be activated
 * sequentially. As the active downstream finishes, the next [[Outlet]] is
 * activated. When the last [[Outlet]] finishes, the entire stream finishes.
 *
 * This stage is designed to buffer a single element in case the downstream
 * outlet is unavailable upon push, but then fails before the in-flight element
 * is pushed to the outlet. The element will be pushed to the next available
 * outlet, if any.
 *
 * If any upstream error occurs, the entire stream fails.
 *
 * @author Philip L. McMahon
 */
class Rollover[T](outputPorts: Int) extends GraphStage[UniformFanOutShape[T, T]] {

  require(outputPorts > 1, "At least two output ports required")

  val shape = new UniformFanOutShape[T, T](outputPorts, "Rollover")

  def createLogic(attr: Attributes) = new GraphStageLogic(shape) {

    var buffer = Option.empty[T]

    var active :: queued = shape.outArray.toList

    val ignorePull = new OutHandler {
      def onPull() = ()
      override def onDownstreamFinish() = queued = queued.filterNot(isClosed(_))
    }

    // Only call when active is known to have pending demand
    def pushAndClearBuffer(elem: T): Unit = {
      push(active, elem)
      buffer = None
    }

    // If no element buffered, relay demand to upstream
    val relayPull: OutHandler = new OutHandler {
      def onPull() = buffer.fold(pull(shape.in)) { elem =>
        push(active, elem)
        buffer = None
      }
      override def onDownstreamFinish() = queued match {
        case first :: rest =>
          setHandler(first, relayPull)
          buffer.foreach { elem =>
            if (isAvailable(first)) {
              push(first, elem)
              buffer = None
            }
          }
          active = first
          queued = rest
        case Nil =>
          completeStage()
      }
    }

    setHandler(shape.in, new InHandler {
      def onPush() = {
        val elem = grab(shape.in)
        if (isAvailable(active)) push(active, elem)
        else buffer = Some(elem)
      }
    })

    setHandler(active, relayPull)

    queued.foreach(setHandler(_, ignorePull))

  }

}

object Rollover {

  def apply[T](n: Int = 2): Rollover[T] = new Rollover[T](n)

}
