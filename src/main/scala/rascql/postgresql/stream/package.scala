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

import rascql.postgresql.protocol._

package stream {

  sealed abstract class StreamException(msg: String) extends RuntimeException(msg)

  class UnsupportedAuthenticationRequestException(request: AuthenticationRequest)
    extends StreamException(s"Authentication request ${request.getClass.getSimpleName} is not supported")

  class AuthenticationFailedException(errors: Seq[ErrorResponse.Field])
    extends StreamException(s"Authentication failed (${errors.mkString(", ")})")

  class UnexpectedBackendMessageException(message: BackendMessage)
    extends StreamException(s"Unexpected backend message with type '${message.getClass.getSimpleName}'")

}
