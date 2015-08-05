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
 * Creates an ordered sequence of [[Outlet]]s, which will be activated
 * sequentially. As the active downstream finishes, the next [[Outlet]] is
 * activated. When the last [[Outlet]] finishes, the entire stream finishes.
 *
 * If any upstream error occurs, the entire stream fails.
 *
 * @author Philip L. McMahon
 */
class Rollover[T](inputPorts: Int)
  extends FlexiRoute[T, UniformFanOutShape[T, T]](
    new UniformFanOutShape(inputPorts), Attributes.name("Rollover")) {

  import FlexiRoute._

  def createRouteLogic(p: PortT) = new RouteLogic[T] {

    private val states = p.outArray.iterator.map(DemandFrom(_)).map {
      State(_) {
        (ctx, out, elem) =>
          ctx.emit(out)(elem)
          SameState
      }
    }

    // Create a special completion handler that advances to the next state when the current downstream completes
    // FIXME Does this need to handle a non-current downstream finishing?
    override def initialCompletionHandling =
      defaultCompletionHandling.copy(onDownstreamFinish = (ctx, _) => {
        if (states.hasNext) states.next()
        else { ctx.finish(); SameState }
      })

    def initialState = states.next()

  }

}

object Rollover {

  def apply[T](n: Int = 2): Rollover[T] = new Rollover[T](2)

}
