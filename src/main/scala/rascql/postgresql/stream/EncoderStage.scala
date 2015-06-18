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

import java.nio.charset.Charset
import akka.stream.stage._
import akka.util.ByteString
import rascql.postgresql.protocol.{FrontendMessage, StartupMessage}

/**
 * Reads the requested `client_encoding` from the initial [[StartupMessage]]
 * and uses this to encode the initial and all future messages.
 *
 * @author Philip L. McMahon
 */
private[stream] class EncoderStage(charset: Charset)
  extends StatefulStage[FrontendMessage, ByteString] {

  private val `client_encoding` = "client_encoding" -> charset.displayName()

  def starting = new State {

    def onPush(msg: FrontendMessage, ctx: Context[ByteString]) =
      msg match {
        case StartupMessage(user, params) =>
          become(started(charset))
          // Replace existing client encoding value, if any, with UTF8.
          // When the encoding is not specified, the database default value
          // will be used.
          ctx.push(StartupMessage(user, params + `client_encoding`).encode(charset))
        case _ =>
          ctx.fail(UnexpectedFrontendMessage(msg))
      }

  }

  def started(charset: Charset) = new State {

    def onPush(msg: FrontendMessage, ctx: Context[ByteString]) =
      ctx.push(msg.encode(charset))

  }

  def initial = starting

}
