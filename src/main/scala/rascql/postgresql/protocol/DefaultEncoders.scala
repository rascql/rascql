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
trait DefaultEncoders {

  import DefaultEncoders.Constants._
  import scala.language.implicitConversions

  type Encoder[T] = Option[T] => Parameter

  implicit def encodeParameter[T](t: T)(implicit e: Encoder[T]): Parameter = e(Option(t))

  object Nullable {

    def apply[T](fn: T => ByteString): Encoder[T] =
      _.map(fn).fold(Parameter.NULL)(Parameter(_))

    def apply[T](default: Parameter)(fn: (T, Charset) => Array[Byte]): Encoder[T] =
      _.map(fn.curried(_).andThen(ByteString(_))).
        fold(default)(Parameter(_))

    def apply[T](fn: (T, Charset) => Array[Byte]): Encoder[T] =
      Nullable(Parameter.NULL)(fn)

  }

  implicit val StringEncoder: Encoder[String] =
    Nullable { _.getBytes(_) }

  // Defer string conversion until encoded form is requested
  def LazyTextEncoder[T]: Encoder[T] =
    Nullable { _.toString.getBytes(_) }

  implicit val BigDecimalEncoder: Encoder[BigDecimal] =
    LazyTextEncoder

  implicit val BigIntEncoder: Encoder[BigInt] =
    LazyTextEncoder

  implicit val BooleanEncoder: Encoder[Boolean] =
    Nullable { if (_) True else False }

  implicit val ByteArrayEncoder: Encoder[Array[Byte]] =
    Nullable { _.foldLeft(HexPrefix)(_ ++ _.asHex) }

  implicit val ByteEncoder: Encoder[Byte] =
    Nullable { HexPrefix ++ _.asHex }

  implicit val CharEncoder: Encoder[Char] =
    LazyTextEncoder

  // FIXME Inefficient due to creation of SDF
  // TODO Add implicit date formatter driven by connection parameters?
  implicit val DateEncoder: Encoder[java.util.Date] =
    Nullable(NullDate) {
      val sdf = new SimpleDateFormat(DateFormat)
      sdf.setTimeZone(UTC)
      sdf.format(_).getBytes(_)
    }

  implicit val DoubleEncoder: Encoder[Double] =
    LazyTextEncoder

  implicit val FloatEncoder: Encoder[Float] =
    LazyTextEncoder

  implicit val IntEncoder: Encoder[Int] =
    LazyTextEncoder

  implicit val LongEncoder: Encoder[Long] =
    LazyTextEncoder

  implicit val ShortEncoder: Encoder[Short] =
    LazyTextEncoder

}

object DefaultEncoders extends DefaultEncoders {

  private[protocol] object Constants {

    val DateFormat = "yyyy-MM-dd"
    val UTC = java.util.TimeZone.getTimeZone("UTC")
    val True = ByteString("t")
    val False = ByteString("f")
    val HexPrefix = ByteString("\\x")
    // FIXME Does the date format need to match a connection-specific parameter?
    val NullDate = Parameter(ByteString("0000-00-00"))

  }

}
