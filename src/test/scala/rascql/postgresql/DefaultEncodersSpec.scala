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
 * Tests for [[DefaultEncoders]].
 *
 * @author Philip L. McMahon
 */
class DefaultEncodersSpec extends WordSpec with DefaultEncoders {

  val charset = Charset.forName("UTF-8")

  implicit class RichEncodable[T](t: T)(implicit e: Encoder[T]) {
    def asEncodable = e(t)
    def encoded = asEncodable.encode(charset)
  }

  "Implicit encoders" should {

    "encode a string" in {

      "".encoded === ByteString("")
      "ABC".encoded === ByteString("ABC")

    }

    "encode a big decimal" in {

      BigDecimal("0.1").encoded === ByteString("0.1")

    }

    "encode a big int" in {

      BigInt("1").encoded === ByteString("1")

    }

    "encode a boolean" in {

      true.encoded === ByteString("t")
      false.encoded === ByteString("f")

    }

    "encode a byte array" in {

      Array[Byte](0x2, 0x23).encoded === ByteString("\\x0123")

    }

    "encode a byte list" in {

      List[Byte](0x0, 0x1).encoded === ByteString("\\x0001")

    }

    "encode a byte" in {

      0x0.toByte.encoded === ByteString("\\x00")

    }

    "encode a char" in {

      'x'.encoded === ByteString("x")

    }

    "encode a date" in {

      new java.util.Date(0).encoded === ByteString("1970-01-01")
      null.asInstanceOf[java.util.Date].encoded === ByteString("0000-00-00")

    }

    "encode a double" in {

      0.1d.encoded === ByteString("0.1")

    }

    "encode a float" in {

      0.1f.encoded === ByteString("0.1")

    }

    "encode an int" in {

      0.encoded === ByteString("0")

    }

    "encode a long" in {

      0L.encoded === ByteString("0")

    }

    "encode a short" in {

      0.toShort.encoded === ByteString("0")

    }

  }

}
