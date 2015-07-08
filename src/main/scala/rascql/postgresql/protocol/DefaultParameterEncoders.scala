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
trait DefaultParameterEncoders {

  import CodecConstants._
  import scala.language.implicitConversions

  // FIXME Does the date format need to match a connection-specific parameter?
  private val NullDate = Parameter(ByteString("0000-00-00"))
  private val HexPrefix = ByteString("\\x")

  type ParameterEncoder[T] = Option[T] => Parameter

  implicit def encodeParameter[T](t: T)(implicit e: ParameterEncoder[T]): Parameter = e(Option(t))

  object Nullable {

    def apply[T](fn: T => ByteString): ParameterEncoder[T] =
      _.map(fn).fold(Parameter.NULL)(Parameter(_))

    def apply[T](default: Parameter)(fn: (T, Charset) => Array[Byte]): ParameterEncoder[T] =
      _.map(fn.curried(_).andThen(ByteString(_))).
        fold(default)(Parameter(_))

    def apply[T](fn: (T, Charset) => Array[Byte]): ParameterEncoder[T] =
      Nullable(Parameter.NULL)(fn)

  }

  implicit val StringEncoder: ParameterEncoder[String] =
    Nullable { _.getBytes(_) }

  // Defer string conversion until encoded form is requested
  def LazyTextEncoder[T]: ParameterEncoder[T] =
    Nullable { _.toString.getBytes(_) }

  implicit val BigDecimalEncoder: ParameterEncoder[BigDecimal] =
    LazyTextEncoder

  implicit val BigIntEncoder: ParameterEncoder[BigInt] =
    LazyTextEncoder

  implicit val BooleanEncoder: ParameterEncoder[Boolean] =
    Nullable { if (_) True else False }

  implicit val ByteArrayEncoder: ParameterEncoder[Array[Byte]] =
    Nullable { _.foldLeft(HexPrefix)(_ ++ _.asHex) }

  implicit val ByteEncoder: ParameterEncoder[Byte] =
    Nullable { HexPrefix ++ _.asHex }

  implicit val CharEncoder: ParameterEncoder[Char] =
    LazyTextEncoder

  // FIXME Inefficient due to creation of SDF
  // TODO Add implicit date formatter driven by connection parameters?
  implicit val DateEncoder: ParameterEncoder[java.util.Date] =
    Nullable(NullDate) {
      val sdf = new SimpleDateFormat(DateFormat)
      sdf.setTimeZone(UTC)
      sdf.format(_).getBytes(_)
    }

  implicit val DoubleEncoder: ParameterEncoder[Double] =
    LazyTextEncoder

  implicit val FloatEncoder: ParameterEncoder[Float] =
    LazyTextEncoder

  implicit val IntEncoder: ParameterEncoder[Int] =
    LazyTextEncoder

  implicit val LongEncoder: ParameterEncoder[Long] =
    LazyTextEncoder

  implicit val ShortEncoder: ParameterEncoder[Short] =
    LazyTextEncoder

}

object DefaultParameterEncoders extends DefaultParameterEncoders
