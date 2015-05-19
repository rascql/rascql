/*
 * Copyright 2014 Philip L. McMahon
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

package rascql.postgresql

import scala.collection.immutable
import scala.util.control.NoStackTrace
import rascql.postgresql.protocol._

package stream {

  sealed trait SendQuery

  object SendQuery {

    def apply(s: String): SendQuery = SendSimpleQuery(s)

    def Prepared(s: String, p: Parameter*): SendQuery = Prepared(s, p.toList)

    def Prepared(s: String, p: Iterable[Parameter]): SendQuery = SendExtendedQuery(s, p.toList)

  }

  private[stream] case class SendSimpleQuery(statement: String) extends SendQuery

  private[stream] case class SendExtendedQuery(statement: String, parameters: immutable.Seq[Parameter]) extends SendQuery

  sealed abstract class StreamException(msg: String)
    extends RuntimeException(msg) with NoStackTrace

  @SerialVersionUID(1)
  case class UnsupportedAuthenticationRequest(request: AuthenticationRequest)
    extends StreamException(s"Authentication request ${request.getClass.getSimpleName} is not supported")

  @SerialVersionUID(1)
  case class AuthenticationFailed(errors: ErrorResponse.Fields)
    extends StreamException(s"Authentication failed (${errors.mkString(", ")})")

  @SerialVersionUID(1)
  case class UnexpectedBackendMessage(message: BackendMessage)
    extends StreamException(s"Unexpected backend message with type '${message.getClass.getSimpleName}'")

  @SerialVersionUID(1)
  case class UnexpectedFrontendMessage(message: FrontendMessage)
    extends StreamException(s"Unexpected frontend message with type '${message.getClass.getSimpleName}'")

}
