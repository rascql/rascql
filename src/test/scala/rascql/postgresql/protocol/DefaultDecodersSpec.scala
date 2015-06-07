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

package rascql.postgresql.protocol

import java.nio.charset.Charset
import akka.util.ByteString
import org.scalatest._

/**
 * Tests for [[DefaultDecoders]].
 *
 * @author Philip L. McMahon
 */
class DefaultDecodersSpec extends WordSpec with Matchers with DefaultDecoders {

  val charset = Charset.forName("UTF-8")

  "Implicit decoders" should {

    "decode a string" in {

      "" shouldDecodeTo ""
      "ABC" shouldDecodeTo "ABC"

    }

    "decode a big decimal" in {

      "0.1" shouldDecodeTo BigDecimal("0.1")

    }

    "decode a big int" in {

      "0" shouldDecodeTo BigInt("0")

    }

    "decode a boolean" in {

      "t" shouldDecodeTo true
      "f" shouldDecodeTo false

    }

    "decode a byte array" in {

      "\\x01237f" shouldDecodeTo Array[Byte](0x1, 0x23, 0x7F)

    }

    "decode a byte" in {

      "\\xFF" shouldDecodeTo 0xFF.toByte

    }

    "decode a char" in {

      " " shouldDecodeTo ' '

    }

    "decode a date" in {

      "1970-01-01" shouldDecodeTo new java.util.Date(0)

    }

    "decode a double" in {

      "0.1" shouldDecodeTo 0.1d

    }

    "decode a float" in {

      "0.1" shouldDecodeTo 0.1f

    }

    "decode an int" in {

      "0" shouldDecodeTo 0

    }

    "decode a long" in {

      "0" shouldDecodeTo 0L

    }

    "decode a short" in {

      "0" shouldDecodeTo 0.toShort

    }

  }

  implicit class RichString(s: String) {

    def shouldDecodeTo[T](right: T)(implicit d: Decoder[T]): T =
      DataRow.Column(Some(ByteString(s)), charset).as[T]

  }

}
