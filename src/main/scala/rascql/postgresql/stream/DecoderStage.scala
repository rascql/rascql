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
import scala.util.{Failure, Success}
import akka.stream.stage._
import akka.util.ByteString
import rascql.postgresql.protocol._

/**
 * Decodes one or more [[BackendMessage]]s from a [[ByteString]].
 *
 * @author Philip L. McMahon
 */
case class DecoderStage(c: Charset, maxLength: Int) extends StatefulStage[ByteString, BackendMessage] {

  def initial = new StageState[ByteString, BackendMessage] {

      var buffered = ByteString.empty

      override def onPush(b: ByteString, ctx: Context[BackendMessage]) =
        BackendMessage.decodeAll(c, maxLength, b.concat(buffered)) match {
          case Success((messages, remainder)) =>
            buffered = remainder.compact
            if (messages.nonEmpty) emit(messages.iterator, ctx)
            else ctx.pull() // Need more data
          case Failure(e) =>
            ctx.fail(e)
        }
    }

}
