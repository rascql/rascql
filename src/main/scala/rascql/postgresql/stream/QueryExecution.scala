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
 * Executes simple and extended (prepared) queries, backpressuring when a query
 * is received and resuming when the server is ready for the next query.
 *
 * A [[Terminate]] is sent when the [[SendQuery]] source finishes, tearing down
 * the stream.
 *
 * {{{
 *                      . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .   . . . . . . . . . . . .
 *                      .                                                                                               .
 *                      .                           +---------------+       +---------------+       +---------------+   .
 *                      .                           |               |       |               |       |               |   .
 *           SendQuery --------------------------> [i]     zip     [o] --> [i]   queries   [o] --> [i]   concat    [o] --> FrontendMessage
 *                      .                           |               |       |               |       |               |   .
 *                      .                           +------[i]------+       +---------------+       +------[o]------+   .
 *                      .                                   ^                                               ^           .
 *                      .                                   |                                               |           .
 *                      .                           +------[o]------+                               +------[o]------+   .
 *                      .                           |               |                               |               |   .
 *                      .                           |    tokens     |                               |   terminate   |   .
 *                      .                           |               |                               |               |   .
 *                      .                           +------[i]------+                               +---------------+   .
 *                      .                                   ^                                                           .
 *                      .                                   |                                                           .
 *                      .   +---------------+       +------[o]------+                                                   .
 *                      .   |               |       |               |                                                   .
 * Source[QueryResult] <-- [o]   results   [i] <-- [o]  broadcast  [i] <-------------------------------------------------- BackendMessage
 *                      .   |               |       |               |                                                   .
 *                      .   +---------------+       +---------------+                                                   .
 *                      .                                                                                               .
 *                      . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .
 *
 * @author Philip L. McMahon
 */
object QueryExecution {

  import FlowGraph.Implicits._

  def apply(): BidiFlow[SendQuery, FrontendMessage, BackendMessage, Source[QueryResult, Unit], Unit] =
    BidiFlow() { implicit b =>

      // Don't allow executing the next statement unless we've seen the prior statement complete
      // Match statement with a token to before processing the statement
      val zip = b.add(ZipWith[TransactionStatus, SendQuery, SendQuery](Keep.right))
      val tokens = b.add(Flow[BackendMessage].collect { case ReadyForQuery(s) => s })
      val concat = b.add(Concat[FrontendMessage]())
      val broadcast = b.add(Broadcast[BackendMessage](2, eagerCancel = false))
      val queries = b.add(Flow[SendQuery].transform(() => new SendQueryStage))
      val terminate = b.add(Source.single(Terminate))
      // Drop first message, which will be the initial RFQ
      val results = b.add(Flow[BackendMessage].dropWhile(_.isInstanceOf[ReadyForQuery]).splitAfter(_.isInstanceOf[ReadyForQuery]).map(_.transform(() => new QueryResultStage)))

      broadcast ~> results
      broadcast ~> tokens ~> zip.in0
                             zip.out ~> queries ~> concat
                             terminate          ~> concat

      BidiShape(zip.in1, concat.out, broadcast.in, results.outlet)
    } named("QueryExecution")

}
