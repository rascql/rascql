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
 * Splits [[AsyncOperation]]s to a separate [[Sink]].
 *
 * {{{
 *                  . . . . . . . . . . . .
 *                  .                     .
 *                  .   +-------------+   .
 *                  .   |             |   .
 * FrontendMessage --> [i] identity  [o] --> FrontendMessage
 *                  .   |             |   .
 *                  .   +-------------+   .
 *                  .                     .
 *                  .   +-------------+   .
 *                  .   |             |   .
 *  BackendMessage <-- [o] partition [i] <-- BackendMessage
 *                  .   |             |   .
 *                  .   +-----[o]-----+   .
 *                  .          |          .
 *                  . . . . . .|. . . . . .
 *                             v
 *                      +-----[i]-----+
 *                      |             |
 *                      |     ops     |
 *                      |             |
 *                      +-------------+
 * }}}
 *
 * @author Philip L. McMahon
 */
object AsyncOperations {

  import FlowGraph.Implicits._

  def apply[Mat](sink: Sink[AsyncOperation, Mat]): BidiFlow[FrontendMessage, FrontendMessage, BackendMessage, BackendMessage, Mat] =
    BidiFlow(sink) { implicit b => ops =>
      val identity = b.add(Flow[FrontendMessage])
      val partition = b.add(Partition[BackendMessage](_.isInstanceOf[AsyncOperation]))

      partition.matched.map(_.asInstanceOf[AsyncOperation]) ~> ops

      BidiShape(identity.inlet, identity.outlet, partition.in, partition.unmatched)
    } named("AsyncOperations")

}
