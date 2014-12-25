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
import scala.util.control.NonFatal

/**
 * Decodes one or more [[BackendMessage]]s from a [[ByteString]].
 *
 * @author Philip L. McMahon
 */
class MessageDecoder(c: Charset, maxLength: Int) extends StatefulStage[ByteString, BackendMessage] {

  def initial = new StageState[ByteString, BackendMessage] {

      var buffered = ByteString.empty

      override def onPush(b: ByteString, ctx: Context[BackendMessage]): Directive =
        try {
          var decoded = Vector.empty[BackendMessage]
          val iter = buffered.concat(b).iterator
          while (iter.hasNext) {
            val code = iter.getByte
            val msgLength = iter.getInt
            val contentLength = msgLength - 4 // Minus 4 bytes for int
            if (contentLength > maxLength) {
              throw new MessageTooLongException(code, contentLength, maxLength)
            } else if (iter.len >= contentLength) {
              // Consume fixed number of bytes from iterator as sub-iterator
              decoded :+= BackendMessage.decode(code, c, iter.getBytes(contentLength))
            } else {
              // Need more data for this message
              // Free bytes which have already been decoded and consume iterator
              buffered = iter.toByteString.compact
            }
          }
          if (decoded.nonEmpty) emit(decoded.iterator, ctx)
          else ctx.pull() // Need more data
        } catch {
          case NonFatal(e) => ctx.fail(e)
        }
    }

}
