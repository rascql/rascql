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

import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import rascql.postgresql.protocol._

/**
 * Encodes and decodes PostgreSQL messages.
 *
 * {{{
 *                  . . . . . . . . . . . . .
 *                  .                       .
 *                  .   +---------------+   .
 *                  .   |               |   .
 * FrontendMessage --> [i]   encoder   [o] --> ByteString
 *                  .   |               |   .
 *                  .   +---------------+   .
 *                  .                       .
 *                  .   +---------------+   .
 *                  .   |               |   .
 *  BackendMessage <-- [o]   decoder   [i] <-- ByteString
 *                  .   |               |   .
 *                  .   +---------------+   .
 *                  .                       .
 *                  . . . . . . . . . . . . .
 * }}}
 *
 * @author Philip L. McMahon
 */
object Codec {

  def apply(charset: Charset): BidiFlow[FrontendMessage, ByteString, ByteString, BackendMessage, Unit] =
    // TODO Support changing encoding based on ParameterStatus message?
    BidiFlow.fromGraph(GraphDSL.create() { b =>

      val encoder = b.add(new EncoderStage(charset))
      val decoder = b.add(Flow[ByteString].transform(() => new DecoderStage(charset)))

      BidiShape(encoder.in, encoder.out, decoder.in, decoder.out)
    } named("Codec"))

}
