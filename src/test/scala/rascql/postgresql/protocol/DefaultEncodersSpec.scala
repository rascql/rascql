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
 * Tests for [[DefaultEncoders]].
 *
 * @author Philip L. McMahon
 */
class DefaultEncodersSpec extends WordSpec with Matchers with DefaultEncoders {

  val charset = Charset.forName("UTF-8")

  "Implicit encoders" should {

    "encode a string" in {

      "" shouldEncodeTo ""
      "" shouldEncodeToRaw (0x0, 0x0, 0x0, 0x0)
      "ABC" shouldEncodeTo "ABC"
      null.asInstanceOf[String] shouldEncodeToRaw (0xFF, 0xFF, 0xFF, 0xFF)

    }

    "encode a big decimal" in {

      BigDecimal("0.1") shouldEncodeTo "0.1"

    }

    "encode a big int" in {

      BigInt("1") shouldEncodeTo "1"

    }

    "encode a boolean" in {

      true shouldEncodeTo "t"
      false shouldEncodeTo "f"

    }

    "encode a byte array" in {

      Array[Byte](0x1, 0x23, 0x7F) shouldEncodeTo "\\x01237f"

    }

    "encode a byte" in {

      0xFF.toByte shouldEncodeTo "\\xff"

    }

    "encode a char" in {

      'x' shouldEncodeTo "x"

    }

    "encode a date" in {

      new java.util.Date(0) shouldEncodeTo "1970-01-01"
      null.asInstanceOf[java.util.Date] shouldEncodeTo "0000-00-00"

    }

    "encode a double" in {

      0.1d shouldEncodeTo "0.1"

    }

    "encode a float" in {

      0.1f shouldEncodeTo "0.1"

    }

    "encode an int" in {

      0 shouldEncodeTo "0"

    }

    "encode a long" in {

      0L shouldEncodeTo "0"

    }

    "encode a short" in {

      0.toShort shouldEncodeTo "0"

    }

  }

  implicit class RichEncodable[T](t: T)(implicit e: Encoder[T]) {

    def encoded = encodedRaw.drop(4) // Drop length

    def encodedRaw = t.encode(charset)

    def shouldEncodeTo(right: String) =
      t.encoded shouldEqual ByteString(right)

    def shouldEncodeToRaw[T](right: T*)(implicit num: Integral[T]) =
      t.encodedRaw shouldEqual ByteString(right: _*)

  }

}
