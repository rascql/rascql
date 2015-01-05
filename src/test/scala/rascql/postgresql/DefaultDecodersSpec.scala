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

package rascql.postgresql

import java.nio.charset.Charset
import akka.util.ByteString
import org.scalatest._

/**
 * Tests for [[DefaultDecoders]].
 *
 * @author Philip L. McMahon
 */
class DefaultDecodersSpec extends WordSpec with DefaultDecoders {

  val charset = Charset.forName("UTF-8")

  private def decodable(s: String) = Decodable(ByteString(s), charset)

  "Implicit decoders" should {

    "decode a string" in {

      decodable("").as[String] === ""
      decodable("ABC").as[String] === "ABC"

    }

    "decode a big decimal" in {

      decodable("0.1").as[BigDecimal] === BigDecimal("0.1")

    }

    "decode a big int" in {

      decodable("0").as[BigInt] === BigInt("0")

    }

    "decode a boolean" in {

      decodable("t").as[Boolean] === true
      decodable("f").as[Boolean] === false

    }

    "decode a byte array" in {

      decodable("\\x0123").as[Array[Byte]] === Array[Byte](0x1, 0x23)

    }

    "decode a byte" in {

      decodable("\\x00").as[Byte] === 0.toByte

    }

    "decode a char" in {

      decodable(" ").as[Char] === ' '

    }

    "decode a date" in {

      decodable("1970-01-01").as[java.util.Date] === new java.util.Date(0)

    }

    "decode a double" in {

      decodable("0.1").as[Double] === 0.1d

    }

    "decode a float" in {

      decodable("0.1").as[Float] === 0.1f

    }

    "decode an int" in {

      decodable("0").as[Int] === 0

    }

    "decode a long" in {

      decodable("0").as[Long] === 0L

    }

    "decode a short" in {

      decodable("0").as[Short] === 0.toShort

    }

  }

}
