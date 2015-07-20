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
import rascql.postgresql.protocol._

/**
 * Emits all [[BackendMessage]]s received to the first [[Outlet]].
 * Emits the current [[TransactionStatus]] to the second [[Outlet]].
 *
 * When the second outlet finishes, queries execution is complete. Any
 * in-flight [[BackendMessage]]s will continue to be emitted to the first
 * outlet until [[ReadyForQuery]] is received, after which the route finishes.
 *
 * @author Philip L. McMahon
 */
private[stream] class ReadyForQueryRoute
  extends FlexiRoute[BackendMessage, FanOutShape2[BackendMessage, BackendMessage, TransactionStatus]](
    new FanOutShape2(("ReadyForQueryRoute")), Attributes.name("ReadyForQueryRoute")) {

  import FlexiRoute._

  def createRouteLogic(p: PortT) = new RouteLogic[BackendMessage] {

    def initialState = readyForQuery(_.emit(p.out1))

    def readyForQuery(fn: RouteLogicContext => TransactionStatus => Unit): State[Any] =
      State[Any](DemandFromAll(p.outlets)) {
        (ctx, _, elem) =>
          ctx.emit(p.out0)(elem)
          Option(elem).collect { case ReadyForQuery(status) => status }.map(fn(ctx))
          SameState
      }

    override def initialCompletionHandling = CompletionHandling(
      onUpstreamFinish = _ => (),
      onUpstreamFailure = (_, _) => (),
      onDownstreamFinish = (ctx, port) =>
        // When transaction state port completes, finish after next RFQ
        if (port == p.out1) readyForQuery { ctx => _ => ctx.finish() }
        // Otherwise tear down stream
        else { ctx.finish() ; SameState }
    )

  }

}
