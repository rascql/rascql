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
import java.text.SimpleDateFormat
import akka.util.ByteString

/**
 * @author Philip L. McMahon
 */
trait DefaultColumnDecoders {

  import DataRow.Column
  import CodecConstants._

  private val HexPrefix = "\\x"

  type ColumnDecoder[T] = Column => Option[T]

  object ColumnDecoder {

    def apply[T](fn: (ByteString, Charset) => T): ColumnDecoder[T] = {
      case Column(b, c) => b.map(fn(_, c))
    }

    def apply[T](fn: ByteString => T): ColumnDecoder[T] = {
      case Column(b, _) => b.map(fn)
    }

  }

  implicit class RichColumn(c: Column) {

    // TODO Support a "retry" with a different decoder if this attempt fails?
    // Eg, val r = Either[Y, X] = d.as[X].orElse[Y]
    def as[T](implicit d: ColumnDecoder[T]): T = d(c).get

    def asOpt[T](implicit d: ColumnDecoder[T]): Option[T] = d(c)

  }

  implicit val StringDecoder: ColumnDecoder[String] =
    ColumnDecoder { (b, c) => new String(b.toArray, c) }

  object TextDecoder {

    def apply[T](fn: String => T): ColumnDecoder[T] =
      StringDecoder.andThen(_.map(fn))

  }

  implicit val BigDecimalDecoder: ColumnDecoder[BigDecimal] =
    TextDecoder { BigDecimal(_) }

  implicit val BigIntDecoder: ColumnDecoder[BigInt] =
    TextDecoder { BigInt(_) }

  implicit val BooleanDecoder: ColumnDecoder[Boolean] =
    ColumnDecoder { _ == True }

  implicit val ByteArrayDecoder: ColumnDecoder[Array[Byte]] =
    TextDecoder {
      _.stripPrefix("\\x").
        grouped(2).
        map(java.lang.Integer.parseInt(_, 16).toByte).
        toArray
    }

  implicit val ByteDecoder: ColumnDecoder[Byte] =
    ByteArrayDecoder.andThen(_.map(_.head)) // FIXME Fail if more than one byte

  implicit val CharDecoder: ColumnDecoder[Char] =
    TextDecoder { _.head } // FIXME Fail if more than one character

  // FIXME Poor performance
  implicit val DateDecoder: ColumnDecoder[java.util.Date] =
    TextDecoder {
      val sdf = new SimpleDateFormat(DateFormat)
      sdf.setTimeZone(UTC)
      sdf.parse(_)
    }

  implicit val DoubleDecoder: ColumnDecoder[Double] =
    TextDecoder { _.toDouble }

  implicit val FloatDecoder: ColumnDecoder[Float] =
    TextDecoder { _.toFloat }

  implicit val IntDecoder: ColumnDecoder[Int] =
    TextDecoder { _.toInt }

  implicit val LongDecoder: ColumnDecoder[Long] =
    TextDecoder { _.toLong }

  implicit val ShortDecoder: ColumnDecoder[Short] =
    TextDecoder { _.toShort }

  // TODO Add Decoder[T] to Decoder[Option[T]] and other conversions?

}

object DefaultColumnDecoders extends DefaultColumnDecoders
