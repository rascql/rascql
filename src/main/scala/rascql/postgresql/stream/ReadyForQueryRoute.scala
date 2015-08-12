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

    // First element is a RFQ(Idle), which is sent only to transaction outlet
    def initialState = State[Outlet[TransactionStatus]](DemandFrom(p.out1)) {
      case (ctx, out, ReadyForQuery(status)) =>
        ctx.emit(out)(status)
        broadcast
      case _ =>
        SameState
    }

    def broadcast: State[Any] = State[Any](DemandFromAll(p)) {
      (ctx, _, elem) =>
        ctx.emit(p.out0)(elem)
        Option(elem).
          collect { case ReadyForQuery(status) => status }.
          foreach(ctx.emit(p.out1))
        SameState
    }

    // Because the transaction state has torn down, drop the RFQ when received
    // and finish entire stream
    def dropLast = State[Outlet[BackendMessage]](DemandFrom(p.out0)) {
      (ctx, out, elem) =>
        if (elem.isInstanceOf[ReadyForQuery]) ctx.finish()
        else ctx.emit(out)(elem)
        SameState
    }

    override def initialCompletionHandling =
      defaultCompletionHandling.copy(onDownstreamFinish = (ctx, port) =>
        // When transaction state port completes, finish after next RFQ
        if (port == p.out1) dropLast
        // Otherwise tear down stream
        else { ctx.finish() ; SameState }
      )

  }

}
