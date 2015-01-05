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

import java.text.SimpleDateFormat
import scala.util.Try

/**
 * @author Philip L. McMahon
 */
trait DefaultDecoders {

  implicit class RichDecodable(d: Decodable) {

    // TODO Support a "retry" with a different decoder if this attempt fails?
    // Eg, val r = Either[Y, X] = d.as[X].orElse[Y]
    def as[T](implicit r: Decoder[T]): Try[T] = Try(r(d))

  }

  implicit val StringDecoder = Decoder { (b, c) => new String(b.toArray, c) }

  protected object FromStringDecoder {

    def apply[T](fn: String => T): Decoder[T] = StringDecoder.andThen(fn)

  }

  implicit val BigDecimalDecoder = FromStringDecoder(BigDecimal(_))

  implicit val BigIntDecoder = FromStringDecoder(BigInt(_))

  implicit val BooleanDecoder = FromStringDecoder(_ == "t")

  implicit val BytesDecoder = FromStringDecoder(_.stripPrefix("\\x").grouped(2).map(_.toByte).toArray)

  protected object FromBytesDecoder {

    def apply[T](fn: Array[Byte] => T): Decoder[T] = BytesDecoder.andThen(fn)

  }

  implicit val ByteDecoder = FromBytesDecoder(_.head) // FIXME Fail if more than one byte

  implicit val CharDecoder = FromStringDecoder(_.head) // FIXME Fail if more than one character

  // FIXME Poor performance
  implicit val DateDecoder = FromStringDecoder(new SimpleDateFormat("yyyy-MM-dd").parse(_))

  implicit val DoubleDecoder = FromStringDecoder(_.toDouble)

  implicit val FloatDecoder = FromStringDecoder(_.toFloat)

  implicit val IntDecoder = FromStringDecoder(_.toInt)

  implicit val LongDecoder = FromStringDecoder(_.toLong)

  implicit val ShortDecoder = FromStringDecoder(_.toShort)

  // TODO Add Decoder[T] to Decoder[Option[T]] and other conversions?

}

object DefaultDecoders extends DefaultDecoders
