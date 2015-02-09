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

  import scala.language.implicitConversions

  type Encoder[T] = Option[T] => Parameter

  implicit def encodeParameter[T](t: T)(implicit e: Encoder[T]): Parameter = e(Option(t))

  private val UTC = java.util.TimeZone.getTimeZone("UTC")
  private val True = ByteString("t")
  private val False = ByteString("f")
  private val HexPrefix = ByteString("\\x")
  private val HexChunks = 0.to(255).map("%02x".format(_).toUpperCase).map(ByteString(_))
  // FIXME Does the date format need to match a connection-specific parameter?
  private val NullDate = Parameter(ByteString("0000-00-00"))

  private implicit class RichByte(b: Byte) {
    @inline def toHex: ByteString = HexChunks(b & 0xFF)
  }

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
  def LazyToStringEncoder[T]: Encoder[T] =
    Nullable { _.toString.getBytes(_) }

  implicit val BigDecimalEncoder: Encoder[BigDecimal] =
    LazyToStringEncoder

  implicit val BigIntEncoder: Encoder[BigInt] =
    LazyToStringEncoder

  implicit val BooleanEncoder: Encoder[Boolean] =
    Nullable { if (_) True else False }

  implicit val ByteArrayEncoder: Encoder[Array[Byte]] =
    Nullable { _.foldLeft(HexPrefix)(_ ++ _.toHex) }

  implicit val ByteEncoder: Encoder[Byte] =
    Nullable { HexPrefix ++ _.toHex }

  implicit val CharEncoder: Encoder[Char] =
    LazyToStringEncoder

  // FIXME Inefficient due to creation of SDF
  // TODO Add implicit date formatter driven by connection parameters?
  implicit val DateEncoder: Encoder[java.util.Date] =
    Nullable(NullDate) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      sdf.setTimeZone(UTC)
      sdf.format(_).getBytes(_)
    }

  implicit val DoubleEncoder: Encoder[Double] =
    LazyToStringEncoder

  implicit val FloatEncoder: Encoder[Float] =
    LazyToStringEncoder

  implicit val IntEncoder: Encoder[Int] =
    LazyToStringEncoder

  implicit val LongEncoder: Encoder[Long] =
    LazyToStringEncoder

  implicit val ShortEncoder: Encoder[Short] =
    LazyToStringEncoder

}

object DefaultEncoders extends DefaultEncoders
