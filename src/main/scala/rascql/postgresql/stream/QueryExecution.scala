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
 * the stream. The current in-flight result, if any, will be returned and then
 * the result sink will finish.
 *
 * {{{
 *                      . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .
 *                      .                                                                                               .
 *                      .                                                                           +---------------+   .
 *                      .                                                                           |               |   .
 *                      .                                                                           |   terminate   |   .
 *                      .                                                                           |               |   .
 *                      .                                                                           +------[o]------+   .
 *                      .                                                                                   |           .
 *                      .                                                                                   v           .
 *                      .                           +---------------+       +---------------+       +------[i]------+   .
 *                      .                           |               |       |               |       |               |   .
 *           SendQuery --------------------------> [i]     zip     [o] --> [i]   queries   [o] --> [i]   concat    [o] --> FrontendMessage
 *                      .                           |               |       |               |       |               |   .
 *                      .                           +------[i]------+       +---------------+       +---------------+   .
 *                      .                                   ^                                                           .
 *                      .                                   |                                                           .
 *                      .   +---------------+       +------[o]------+                                                   .
 *                      .   |               |       |               |                                                   .
 * Source[QueryResult] <-- [o]   results   [i] <-- [o]     rfq     [i] <-------------------------------------------------- BackendMessage
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
      val rfq = b.add(new ReadyForQueryRoute)
      val concat = b.add(Concat[FrontendMessage]())
      val queries = b.add(Flow[SendQuery].transform(() => new SendQueryStage))
      val terminate = b.add(Source.single(Terminate))
      val results = b.add(Flow[BackendMessage].splitAfter(_.isInstanceOf[ReadyForQuery]).map(_.transform(() => new QueryResultStage)))

      rfq.out0 ~> results
      rfq.out1 ~> zip.in0
                  zip.out ~> queries ~> concat
                  terminate          ~> concat

      BidiShape(zip.in1, concat.out, rfq.in, results.outlet)
    } named("QueryExecution")

}
