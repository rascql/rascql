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
 * If any upstream error occurs, the entire stream fails.
 *
 * @author Philip L. McMahon
 */
class Rollover[T](outputPorts: Int) extends GraphStage[UniformFanOutShape[T, T]] {

  require(outputPorts > 1, "At least two output ports required")

  val shape = new UniformFanOutShape[T, T](outputPorts, "Rollover")

  def createLogic(attr: Attributes) = new GraphStageLogic(shape) {

    // Lazily evaluate state
    val inHandlers = shape.outArray.iterator.map { outlet =>
      new InHandler {
        def onPush() = push(outlet, grab(shape.in))
      }
    }

    val outHandler = new OutHandler {
      def onPull() = pull(shape.in)
      override def onDownstreamFinish() =
        if (inHandlers.hasNext) setHandler(shape.in, inHandlers.next())
        else completeStage()
    }

    shape.outArray.foreach(setHandler(_, outHandler))

    setHandler(shape.in, inHandlers.next())

  }

}

object Rollover {

  def apply[T](n: Int = 2): Rollover[T] = new Rollover[T](2)

}
