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

import java.nio.charset.Charset
import akka.stream.stage._
import akka.util.ByteString
import rascql.postgresql.protocol._

/**
 * Decodes zero or more [[BackendMessage]]s from a [[ByteString]].
 *
 * @author Philip L. McMahon
 */
private[stream] class DecoderStage(charset: Charset)
  extends PushPullStage[ByteString, BackendMessage] {

  import Decoder._

  var buffer = ByteString.empty

  def onPush(bytes: ByteString, ctx: Context[BackendMessage]) =
    tryDecode(buffer ++ bytes, ctx)

  // This will try to decode message header multiple times if not enough data is available
  def tryDecode(bytes: ByteString, ctx: Context[BackendMessage]) =
    if (bytes.isEmpty) pullOrFinish(ctx)
    else {
      Decoder(bytes.head).decode(charset, bytes.tail) match {
        case _: NeedBytes =>
          pullOrFinish(ctx)
        case MessageDecoded(msg, rest) =>
          buffer = rest
          ctx.push(msg)
      }
    }

  def onPull(ctx: Context[BackendMessage]) = tryDecode(buffer, ctx)

  def pullOrFinish(ctx: Context[BackendMessage]) =
    if (ctx.isFinishing) ctx.finish() else ctx.pull()

  override def onUpstreamFinish(ctx: Context[BackendMessage]) =
    if (buffer.isEmpty) ctx.finish() else ctx.absorbTermination()

}
