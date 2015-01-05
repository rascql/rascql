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
import java.text.SimpleDateFormat
import akka.util.ByteString

/**
 * @author Philip L. McMahon
 */
trait DefaultEncoders {

  private val True = ByteString("t")
  private val False = ByteString("f")
  private val HexPrefix = ByteString("\\x")
  private val Zero = ByteString("0")

  // TODO Use code in Password companion object?
  protected implicit class RichByte(b: Byte) {
    def toHex: ByteString = {
      val h = Integer.toHexString(b)
      if (h.length == 1) Zero ++ ByteString(h) else ByteString(h)
    }
  }

  implicit class StringEncoder(s: String) extends Encodable {
    def encode(c: Charset) = ByteString(s.getBytes(c))
  }

  protected abstract class ToStringEncodable[T](e: T) extends Encodable {
    def encode(c: Charset) = ByteString(e.toString.getBytes(c))
  }

  implicit class BigDecimalEncoder(d: BigDecimal) extends ToStringEncodable(d)

  implicit class BigIntEncoder(i: BigInt) extends ToStringEncodable(i)

  implicit class BooleanEncoder(b: Boolean) extends Encodable {
    def encode(c: Charset) = if (b) True else False
  }

  implicit class ByteArrayEncoder(b: Array[Byte]) extends BytesEncoder(b)

  implicit class BytesEncoder(b: Traversable[Byte]) extends Encodable {
    def encode(c: Charset) = b.foldLeft(HexPrefix)(_ ++ _.toHex)
  }

  implicit class ByteEncoder(b: Byte) extends Encodable {
    def encode(c: Charset) = HexPrefix ++ b.toHex
  }

  implicit class CharEncoder(c: Char) extends ToStringEncodable(c)

  // FIXME Not very efficient
  implicit class DateEncoder(d: java.util.Date) extends Encodable {
    def encode(c: Charset) = ByteString(
      Option(d).
        fold("0000-00-00")(new SimpleDateFormat("yyyy-MM-dd").format).
        getBytes(c)
    )
  }

  implicit class DoubleEncoder(d: Double) extends ToStringEncodable(d)

  implicit class FloatEncoder(f: Float) extends ToStringEncodable(f)

  implicit class IntEncoder(i: Int) extends ToStringEncodable(i)

  implicit class LongEncoder(l: Long) extends ToStringEncodable(l)

  implicit class ShortEncoder(s: Short) extends ToStringEncodable(s)

}

object DefaultEncoders extends DefaultEncoders
