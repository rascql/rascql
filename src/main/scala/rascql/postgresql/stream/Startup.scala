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

import scala.collection.immutable
import akka.stream.FlowShape
import akka.stream.scaladsl._
import rascql.postgresql.protocol._

/**
 * Initiates the client/server communication via a [[StartupMessage]] and then
 * handles any subsequent [[AuthenticationRequest]]s.
 *
 * {{{
 *                 . . . . . . . . . . . . . . . . . . . . . . . . .
 *                 .                                               .
 *                 .                           +---------------+   .
 *                 .                           |               |   .
 *                 .                           |    initial    |   .
 *                 .                           |               |   .
 *                 .                           +------[o]------+   .
 *                 .                                   |           .
 *                 .                                   v           .
 *                 .   +---------------+       +------[i]------+   .
 *                 .   |               |       |               |   .
 * BackendMessage --> [i]    authn    [o] --> [i]   concat    [o] --> FrontendMessage
 *                 .   |               |       |               |   .
 *                 .   +---------------+       +---------------+   .
 *                 .                                               .
 *                 . . . . . . . . . . . . . . . . . . . . . . . . .
 *
 * @author Philip L. McMahon
 */
object Startup {

  type Parameters = immutable.Map[String, String]

  def apply(username: String, password: String, parameters: Parameters): Flow[BackendMessage, FrontendMessage, Unit] =
    Flow[BackendMessage].transform(() => new AuthenticationStage(username, password)).
      prepend(Source.single[FrontendMessage](StartupMessage(username, parameters))).
      named("Startup")

}
