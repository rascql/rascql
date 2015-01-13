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

package rascql.postgresql.stream

import akka.stream.stage._
import rascql.postgresql.protocol._

/**
 * Handles the authentication phase of a connection.
 *
 * @author Philip L. McMahon
 */
case class AuthenticationStage(username: String, password: String) extends PushStage[BackendMessage, FrontendMessage] {

  def onPush(msg: BackendMessage, ctx: Context[FrontendMessage]) = msg match {
    case AuthenticationOk =>
      ctx.finish()
    case AuthenticationCleartextPassword =>
      ctx.push(PasswordMessage(Password.ClearText(password)))
    case AuthenticationMD5Password(salt) =>
      ctx.push(PasswordMessage(Password.MD5(username, password, salt)))
    case r: AuthenticationRequest =>
      ctx.fail(UnsupportedAuthenticationRequest(r))
    case ErrorResponse(fields) =>
      ctx.fail(AuthenticationFailed(fields))
    case _ =>
      ctx.fail(UnexpectedBackendMessage(msg))
  }

}