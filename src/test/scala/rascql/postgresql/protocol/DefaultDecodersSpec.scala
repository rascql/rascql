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

  private def column(s: String) = Column(Some(ByteString(s)), charset)

  "Implicit decoders" should {

    "decode a string" in {

      column("").as[String] shouldEqual ""
      column("ABC").as[String] shouldEqual "ABC"

    }

    "decode a big decimal" in {

      column("0.1").as[BigDecimal] shouldEqual BigDecimal("0.1")

    }

    "decode a big int" in {

      column("0").as[BigInt] shouldEqual BigInt("0")

    }

    "decode a boolean" in {

      column("t").as[Boolean] shouldEqual true
      column("f").as[Boolean] shouldEqual false

    }

    "decode a byte array" in {

      column("\\x01237f").as[Array[Byte]] shouldEqual Array[Byte](0x1, 0x23, 0x7F)

    }

    "decode a byte" in {

      column("\\xFF").as[Byte] shouldEqual 255.toByte

    }

    "decode a char" in {

      column(" ").as[Char] shouldEqual ' '

    }

    "decode a date" in {

      column("1970-01-01").as[java.util.Date] shouldEqual new java.util.Date(0)

    }

    "decode a double" in {

      column("0.1").as[Double] shouldEqual 0.1d

    }

    "decode a float" in {

      column("0.1").as[Float] shouldEqual 0.1f

    }

    "decode an int" in {

      column("0").as[Int] shouldEqual 0

    }

    "decode a long" in {

      column("0").as[Long] shouldEqual 0L

    }

    "decode a short" in {

      column("0").as[Short] shouldEqual 0.toShort

    }

  }

}
