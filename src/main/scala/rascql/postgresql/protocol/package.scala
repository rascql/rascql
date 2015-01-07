/*
 * Copyright 2014 Philip L. McMahon
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

import java.nio.ByteOrder
import java.nio.charset.Charset
import java.security.MessageDigest
import scala.annotation.switch
import scala.collection.immutable
import scala.util.Try
import akka.util._

package object protocol {

  type OID = Int
  type ProcessID = Int
  type SecretKey = Int

  private[protocol] implicit val order = ByteOrder.BIG_ENDIAN

  private[protocol] val NUL = 0x0.toByte

  private[protocol] implicit class RichByteIterator(val b: ByteIterator) extends AnyVal {

    def getCString(c: Charset): String = {
      val iter = b.clone
      val result = iter.takeWhile(_ != NUL) // FIXME Throw error if no NUL found (result length equals iter length)
      b.drop(result.len + 1) // Consume up to and including NUL byte
      new String(result.toArray[Byte], c)
    }

    // Since take/slice both truncate the iterator and we want to return a sub-iterator for a given range, we do this instead.
    def nextBytes(n: Int): ByteIterator = {
      val iter = b.clone
      b.drop(n)
      iter.take(n)
    }

  }

  private[protocol] implicit class RichByteStringBuilder(val b: ByteStringBuilder) extends AnyVal {

    @inline def putCString(content: String, charset: Charset): ByteStringBuilder =
      b.putBytes(content.getBytes(charset)).putNUL

    @inline def putNUL: ByteStringBuilder = b.putByte(NUL)

    def prependLength: ByteStringBuilder =
      ByteString.newBuilder.
        putInt(b.length + 4). // Include length of int
        append(b.result)

  }

  private[protocol] implicit class RichByteString(val b: ByteString) extends AnyVal {

    def prependLength: ByteString =
      ByteString.newBuilder.putInt(b.length).append(b).result

  }

  private[protocol] implicit class RichOptionOfFieldFormats(val f: Option[FieldFormats]) extends AnyVal {
    def encoded: ByteString = f.fold(FieldFormats.Default)(_.encoded)
  }

  private[protocol] implicit class RichSeqOfParameter(val p: Seq[Parameter]) extends AnyVal {
    def encode(c: Charset): ByteString =
      ByteString.newBuilder.
        putShort(p.size).
        putShorts(p.map(_.format.toShort).toArray).
        putShort(p.size).
        append(p.foldLeft(ByteString.empty)(_ ++ _.encode(c))).
        result
  }

}

package protocol {

  sealed trait FrontendMessage {

    def encode(c: Charset): ByteString

  }

  object FrontendMessage {

    sealed abstract class Empty(typ: Byte) extends FrontendMessage {

      private val data = ByteString.newBuilder.putByte(typ).putInt(4).result

      def encode(c: Charset) = data

    }

    sealed abstract class NonEmpty(typ: Byte) extends FrontendMessage {

      final def encode(c: Charset) = {
        val content = encodeContent(c)
        ByteString.newBuilder.
          putByte(typ).
          putInt(content.length + 4).
          append(content).
          result
      }

      protected def encodeContent(c: Charset): ByteString

    }

  }

  sealed trait BackendMessage

  object BackendMessage {

    trait Empty extends BackendMessage with Decoder {

      def decode(c: Charset, b: ByteIterator) = this

    }

    def decodeAll(c: Charset, maxLen: Int, bytes: ByteString): Try[(immutable.Seq[BackendMessage], ByteString)] = {
      Try {
        var decoded = Vector.empty[BackendMessage]
        var remaining = ByteString.empty
        val iter = bytes.iterator
        while (iter.hasNext) {
          val code = iter.getByte
          val msgLength = iter.getInt
          val contentLength = msgLength - 4 // Minus 4 bytes for int
          if (contentLength > maxLen) {
            throw new MessageTooLongException(code, contentLength, maxLen)
          } else if (iter.len >= contentLength) {
            val decoder = (code: @switch) match {
              case 'R' => AuthenticationRequest
              case 'K' => BackendKeyData
              case '2' => BindComplete
              case '3' => CloseComplete
              case 'C' => CommandComplete
              case 'd' => CopyData
              case 'c' => CopyDone
              case 'G' => CopyInResponse
              case 'H' => CopyOutResponse
              case 'W' => CopyBothResponse
              case 'D' => DataRow
              case 'I' => EmptyQueryResponse
              case 'E' => ErrorResponse
              case 'V' => FunctionCallResponse
              case 'n' => NoData
              case 'N' => NoticeResponse
              case 'A' => NotificationResponse
              case 't' => ParameterDescription
              case 'S' => ParameterStatus
              case '1' => ParseComplete
              case 's' => PortalSuspended
              case 'Z' => ReadyForQuery
              case 'T' => RowDescription
              case _ => throw new UnsupportedMessageTypeException(code)
            }
            // Consume fixed number of bytes from iterator as sub-iterator
            decoded :+= decoder.decode(c, iter.nextBytes(contentLength))
          } else {
            // Need more data for this message
            // Free bytes which have already been decoded and consume iterator
            remaining = iter.toByteString // TODO compact?
          }
        }
        decoded -> remaining
      }
    }


  }

  sealed abstract class AuthenticationRequest extends BackendMessage

  object AuthenticationRequest extends Decoder {

    def decode(c: Charset, b: ByteIterator) = {
      (b.getInt: @switch) match {
        case 0 => AuthenticationOk
        case 2 => AuthenticationKerberosV5
        case 3 => AuthenticationCleartextPassword
        case 5 => AuthenticationMD5Password(b.toByteString) // TODO compact?
        case 6 => AuthenticationSCMCredential
        case 7 => AuthenticationGSS
        case 8 => AuthenticationGSSContinue(b.toByteString) // TODO compact?
        case 9 => AuthenticationSSPI
        case m => throw new UnsupportedAuthenticationMethodException(m)
      }
    }

  }

  case object AuthenticationOk extends AuthenticationRequest
  case object AuthenticationKerberosV5 extends AuthenticationRequest
  case object AuthenticationCleartextPassword extends AuthenticationRequest
  case class AuthenticationMD5Password(salt: ByteString) extends AuthenticationRequest
  case object AuthenticationSCMCredential extends AuthenticationRequest
  case object AuthenticationGSS extends AuthenticationRequest
  case class AuthenticationGSSContinue(data: ByteString) extends AuthenticationRequest
  case object AuthenticationSSPI extends AuthenticationRequest

  case class BackendKeyData(processId: ProcessID, secretKey: SecretKey) extends BackendMessage

  object BackendKeyData extends Decoder {

    def decode(c: Charset, b: ByteIterator) =
      BackendKeyData(b.getInt, b.getInt)

  }

  case class Bind(parameters: Seq[Parameter],
                  destination: Portal = Portal.Unnamed,
                  source: PreparedStatement = PreparedStatement.Unnamed,
                  resultFormats: Option[FieldFormats] = None) extends FrontendMessage.NonEmpty('B') {

    protected def encodeContent(c: Charset) =
      ByteString.newBuilder.
        putCString(destination.name, c).
        putCString(source.name, c).
        append(parameters.encode(c)).
        append(resultFormats.encoded).
        result

  }

  case object BindComplete extends BackendMessage.Empty

  case class CancelRequest(processId: ProcessID, secretKey: SecretKey) extends FrontendMessage {

    private val prefix = ByteString.newBuilder.putInt(16).putInt(80877102).result

    def encode(c: Charset) = prefix ++
      ByteString.newBuilder.
        putInt(processId).
        putInt(secretKey).
        result

  }

  case class Close(target: Closable) extends FrontendMessage.NonEmpty('C') {

    protected def encodeContent(c: Charset) = target.encode(c)

  }

  case object CloseComplete extends BackendMessage.Empty

  case class CommandComplete(tag: CommandTag) extends BackendMessage

  object CommandComplete extends Decoder {

    import CommandTag._

    // TODO Use a Try to avoid exceptions/invalid data
    def decode(c: Charset, b: ByteIterator) = {
      val raw = b.getCString(c)
      val (Array(name), args) = raw.split(" ").splitAt(1)
      val tag = (name -> args.map(_.toInt)) match {
        case ("INSERT", Array(oid, rows)) =>
          // TODO Verify large unsigned OID parses properly
          Insert(oid, rows)
        case ("DELETE", Array(rows)) => Delete(rows.toInt)
        case ("UPDATE", Array(rows)) => Update(rows.toInt)
        case ("SELECT", Array(rows)) => Select(rows.toInt)
        case ("MOVE", Array(rows)) => Move(rows.toInt)
        case ("FETCH", Array(rows)) => Fetch(rows.toInt)
        case ("COPY", Array(rows)) => Copy(Some(rows.toInt)) // 8.2 and later
        case ("COPY", _) => Copy(None) // Pre-8.2
        case ("BEGIN", _) => Begin
        case ("ROLLBACK", _) => Rollback
        case ("COMMIT", _) => Commit
        case _ => Unknown(raw)
      }
      // Expect row count (require PostgreSQL 8.2 or later for copy command)
      CommandComplete(tag)
      //    val rows = Try(it.next().toInt)
      //    CommandComplete(tag, rows.getOrElse(0))
    }

  }

  case class CopyData(value: ByteString) extends FrontendMessage.NonEmpty('d') with BackendMessage {

    protected def encodeContent(c: Charset) = value

  }

  object CopyData extends Decoder {

    def decode(c: Charset, b: ByteIterator) =
      CopyData(b.toByteString) // TODO compact?

  }

  case object CopyDone extends FrontendMessage.Empty('c') with BackendMessage.Empty

  case class CopyFail(error: String) extends FrontendMessage.NonEmpty('f') {

    protected def encodeContent(c: Charset) =
      ByteString.newBuilder.
        putCString(error, c).
        result

  }

  abstract class CopyResponse extends BackendMessage {

    def format: FieldFormats

  }

  sealed abstract class CopyResponseDecoder(typ: Byte) extends Decoder {

    import Format._
    import FieldFormats._

    def decode(c: Charset, b: ByteIterator) = {
      val format = Format.decode(b.getByte)
      val size = b.getShort
      val types = Vector.fill(size)(b.getByte).map(Format.decode(_))
      apply(format match {
        case Text =>
          // All columns must have format text
          val invalid = types.
            zipWithIndex.
            filter(_._1 == Format.Binary)

          if (invalid.nonEmpty) {
            throw new TextOnlyCopyFormatException(invalid.map(_._2))
          } else {
            Matched(format, size)
          }
        case Binary =>
          Mixed(types)
      })
    }

    def apply(format: FieldFormats): CopyResponse

  }

  case class CopyInResponse(format: FieldFormats) extends CopyResponse

  object CopyInResponse extends CopyResponseDecoder('G')

  case class CopyOutResponse(format: FieldFormats) extends CopyResponse

  object CopyOutResponse extends CopyResponseDecoder('H')

  case class CopyBothResponse(format: FieldFormats) extends CopyResponse

  object CopyBothResponse extends CopyResponseDecoder('W')

  case class DataRow(values: immutable.IndexedSeq[Option[Decodable]]) extends BackendMessage

  object DataRow extends Decoder {

    def decode(c: Charset, b: ByteIterator) =
      DataRow((0 until b.getShort) map { _ =>
        Option(b.getInt).
          filterNot(_ < 0).
          map(b.nextBytes(_).toByteString). // TODO compact?
          map(Decodable(_, c))
      })

  }

  case class Describe(target: Closable)  extends FrontendMessage.NonEmpty('D') {

    protected def encodeContent(c: Charset) = target.encode(c)

  }

  case object EmptyQueryResponse extends BackendMessage.Empty

  case class ErrorResponse(fields: immutable.Seq[ErrorResponse.Field]) extends BackendMessage

  object ErrorResponse extends Decoder with ResponseFields {

    def decode(c: Charset, b: ByteIterator) =
      ErrorResponse(decodeAll(c, b))

  }

  case class Execute(portal: Portal, maxRows: Option[Int] = None) extends FrontendMessage.NonEmpty('E') {

    protected def encodeContent(c: Charset) =
      ByteString.newBuilder.
        putCString(portal.name, c).
        putInt(maxRows.getOrElse(0)).
        result

  }

  case object Flush extends FrontendMessage.Empty('H')

  case class FunctionCall(target: OID,
                          arguments: Seq[Parameter],
                          result: Format) extends FrontendMessage.NonEmpty('F') {

    protected def encodeContent(c: Charset) =
      ByteString.newBuilder.
        putInt(target).
        append(arguments.encode(c)).
        putShort(result.toShort).
        result

  }

  case class FunctionCallResponse(value: Option[ByteString]) extends BackendMessage

  object FunctionCallResponse extends Decoder {

    def decode(c: Charset, b: ByteIterator) =
      FunctionCallResponse(
        Option(b.getInt).
          filter(_ > 0).
          map(b.nextBytes(_).toByteString) // TODO compact?
      )

  }

  case object NoData extends BackendMessage.Empty

  case class NoticeResponse(fields: immutable.Seq[NoticeResponse.Field]) extends BackendMessage

  object NoticeResponse extends Decoder with ResponseFields {

    def decode(c: Charset, b: ByteIterator) =
      NoticeResponse(decodeAll(c, b))

  }

  case class NotificationResponse(processId: Int, channel: String, payload: String) extends BackendMessage

  object NotificationResponse extends Decoder {

    def decode(c: Charset, b: ByteIterator) =
      NotificationResponse(b.getInt, b.getCString(c), b.getCString(c))

  }

  case class ParameterDescription(types: immutable.IndexedSeq[OID]) extends BackendMessage

  object ParameterDescription extends Decoder {

    def decode(c: Charset, b: ByteIterator) =
      ParameterDescription(Vector.fill(b.getShort)(b.getInt))

  }

  case class ParameterStatus(key: String, value: String) extends BackendMessage

  object ParameterStatus extends Decoder {

    def decode(c: Charset, b: ByteIterator) =
      ParameterStatus(b.getCString(c), b.getCString(c))

  }

  case class Parse(query: String,
                   types: Seq[OID] = Nil,
                   destination: PreparedStatement = PreparedStatement.Unnamed) extends FrontendMessage.NonEmpty('P') {

    protected def encodeContent(c: Charset) =
      ByteString.newBuilder.
        putCString(destination.name, c).
        putCString(query, c).
        putShort(types.size).
        putInts(types.toArray).
        result

  }

  case object ParseComplete extends BackendMessage.Empty

  case class PasswordMessage(password: Password) extends FrontendMessage.NonEmpty('p') {

    protected def encodeContent(c: Charset) = password.encode(c)

  }

  case object PortalSuspended extends BackendMessage.Empty

  case class Query(str: String) extends FrontendMessage.NonEmpty('Q') {

    protected def encodeContent(c: Charset) =
      ByteString.newBuilder.
        putCString(str, c).
        result

  }

  case class ReadyForQuery(status: TransactionStatus) extends BackendMessage

  object ReadyForQuery extends Decoder {

    import TransactionStatus._

    def decode(c: Charset, b: ByteIterator) =
      ReadyForQuery(
        (b.getByte: @switch) match {
          case 'I' => Idle
          case 'T' => Open
          case 'E' => Failed
          case s => throw new UnsupportedTransactionStatusException(s)
        }
      )

  }

  case class RowDescription(fields: immutable.IndexedSeq[RowDescription.Field]) extends BackendMessage

  object RowDescription extends Decoder {

    case class Field(name: String,
                     tableOid: OID,
                     column: Int,
                     dataType: DataType,
                     format: Format)

    case class DataType(oid: OID, size: Long, modifier: Int)

    def decode(c: Charset, b: ByteIterator) = {
      RowDescription(
        (0 until b.getShort).map { index =>
          Field(
            name = b.getCString(c),
            tableOid = b.getInt,
            column = b.getShort,
            dataType = DataType(
              oid = b.getInt,
              size = b.getShort,
              modifier = b.getInt
            ),
            format = Format.decode(b.getShort) // FIXME When returned after describe, will always be zero for "unknown"
          )
        }
      )
    }

  }

  case object SSLRequest extends FrontendMessage {

    private val data =
      ByteString.newBuilder.
        putInt(8). // total size
        putInt(80877103).
        result

    def encode(c: Charset) = data

    sealed trait Reply

    case object Accepted extends Reply

    case object Rejected extends Reply

    object Reply {

      def decode(b: Byte): Reply = (b: @switch) match {
        case 'S' => Accepted
        case 'N' => Rejected
        case _ =>
          throw new UnsupportedSSLReplyException(b)
      }

    }

  }

  case class StartupMessage(user: String, parameters: Map[String, String]) extends FrontendMessage {

    private val userParam = "user"

    def encode(c: Charset) =
      ByteString.newBuilder.
        putInt(196608). // version 3.0
        putCString(userParam, c).
        putCString(user, c).
        append(parameters.filterKeys(_ != userParam).foldLeft(ByteString.newBuilder) {
          case (b, (k, v)) =>
            b.putCString(k, c).
              putCString(v, c)
        }.result).
        putNUL.
        prependLength.
        result

  }

  case object Sync extends FrontendMessage.Empty('S')

  case object Terminate extends FrontendMessage.Empty('X')

  trait Decoder {

    def decode(c: Charset, b: ByteIterator): BackendMessage

  }

  // Note that the PostgreSQL documentation recommends only encoding parameters
  // using the text format, since it is portable across versions.
  case class Parameter(value: Option[Encodable], format: Format = Format.Text) {

    def encode(c: Charset): ByteString =
      value.fold(Parameter.NULL)(_.encode(c).prependLength)

  }

  object Parameter {

    private val NULL = ByteString.newBuilder.putInt(-1).result

    // If value is null, Parameter argument will be None
    def apply(value: Encodable): Parameter = Parameter(Option(value))

  }

  sealed trait CommandTag

  object CommandTag {

    // FIXME Should the rows count be a long?
    case class Insert(oid: OID, rows: Int) extends CommandTag // FIXME oid is an unsigned int
    case class Delete(rows: Int) extends CommandTag
    case class Update(rows: Int) extends CommandTag
    case class Select(rows: Int) extends CommandTag
    case class Move(rows: Int) extends CommandTag
    case class Fetch(rows: Int) extends CommandTag
    case class Copy(rows: Option[Int]) extends CommandTag // Pre-8.2, row count unavailable
    case object Begin extends CommandTag
    case object Commit extends CommandTag
    case object Rollback extends CommandTag
    case class Unknown(raw: String) extends CommandTag

  }

  sealed abstract class Closable(typ: Byte) {

    def name: String

    def encode(c: Charset): ByteString =
      ByteString.newBuilder.
        putByte(typ).
        putCString(name, c).
        result

  }

  sealed abstract class Portal extends Closable('P')

  object Portal {

    case class Named(name: String) extends Portal {
      require(name.nonEmpty)
    }

    case object Unnamed extends Portal {
      val name = ""
    }

    def apply(name: String): Portal =
      if (name.isEmpty) Unnamed else Named(name)

    def unapply(p: Portal): Option[String] = Some(p.name)

  }

  // TODO Duplicates format of Portal -- DRY using macro?
  sealed abstract class PreparedStatement extends Closable('S')

  object PreparedStatement {

    case class Named(name: String) extends PreparedStatement {
      require(name.nonEmpty)
    }

    case object Unnamed extends PreparedStatement {
      val name = ""
    }

    def apply(name: String): PreparedStatement =
      if (name.isEmpty) Unnamed else Named(name)

    def unapply(p: PreparedStatement): Option[String] = Some(p.name)

  }

  sealed abstract class Format(typ: Byte) {

    def toShort = typ.toShort

    def toByte = typ

  }

  object Format {

    case object Text extends Format(0)
    case object Binary extends Format(1)

    def decode(typ: Short) = typ match {
      case 0 => Text
      case 1 => Binary
      case _ => throw new UnsupportedFormatTypeException(typ)
    }

  }

  sealed trait FieldFormats {

    def encoded: ByteString

    def apply(index: Short): Format

  }

  object FieldFormats {

    private[protocol] val Default = ByteString.newBuilder.putShort(0).result

    case class Matched(format: Format, count: Short) extends FieldFormats {
      def apply(index: Short) =
        if (index < count) format
        else throw new IndexOutOfBoundsException
      def encoded = ByteString.newBuilder.putShort(1).putShort(format.toShort).result
    }

    case class Mixed(types: immutable.IndexedSeq[Format]) extends FieldFormats {
      def apply(index: Short) = types(index)
      def encoded = ByteString.newBuilder.putShort(types.size).putShorts(types.map(_.toShort).toArray).result
    }

    // TODO Try to detect if Matched can be used
    def apply(types: Iterable[Format]): FieldFormats = Mixed(types.toVector)

  }

  trait Password {

    def encode(c: Charset): ByteString

  }

  object Password {

    case class ClearText(value: String) extends Password {
      def encode(c: Charset) =
        ByteString.newBuilder.
          putCString(value, c).
          result
    }

    case class MD5(username: String, password: String, salt: ByteString) extends Password {

      import MD5._

      def encode(c: Charset) = {
        val md = MessageDigest.getInstance("MD5")
        md.update(password.getBytes(c))
        md.update(username.getBytes(c))
        md.update(md.digest().toHex)
        md.update(salt.toArray)
        ByteString.newBuilder.
          append(md5).
          putBytes(md.digest().toHex).
          putNUL.
          result
      }

    }

    object MD5 {

      private val md5 = ByteString("md5")
      private val hexBytes = (('0' to '9') ++ ('a' to 'f')).map(_.toByte)

      // This isn't very efficient, but it's only used during login
      private implicit class RichArray(val a: Array[Byte]) extends AnyVal {
        def toHex: Array[Byte] = a.map(_ & 0xff).flatMap { c =>
          Array(c >> 4, c & 0xf).map(hexBytes.apply)
        }
      }

    }

  }

  // TODO Make this an inner class of ReadyForQuery object?
  sealed trait TransactionStatus

  object TransactionStatus {

    case object Idle extends TransactionStatus
    case object Open extends TransactionStatus
    case object Failed extends TransactionStatus

  }

  sealed trait ResponseFields {

    sealed trait Field

    case class Severity(level: String) extends Field
    case class SQLState(code: String) extends Field
    case class Message(text: String) extends Field
    case class Detail(text: String) extends Field
    case class Hint(text: String) extends Field
    case class Position(index: Int) extends Field
    case class InternalPosition(index: Int) extends Field
    case class InternalQuery(text: String) extends Field
    case class Where(trace: immutable.IndexedSeq[String]) extends Field
    case class Schema(name: String) extends Field
    case class Table(name: String) extends Field
    case class Column(name: String) extends Field
    case class DataType(name: String) extends Field
    case class Constraint(name: String) extends Field
    case class File(path: String) extends Field
    case class Line(index: Int) extends Field
    case class Routine(name: String) extends Field

    protected def decodeAll(c: Charset, b: ByteIterator): immutable.Seq[Field] =
      Iterator.continually(b.getByte).
        takeWhile(_ != NUL).
        foldLeft(Vector.empty[Field]) { (fields, typ) =>
        val value = b.getCString(c)
        (typ: @switch) match {
          case 'S' => fields :+ Severity(value)
          case 'C' => fields :+ SQLState(value)
          case 'M' => fields :+ Message(value)
          case 'D' => fields :+ Detail(value)
          case 'H' => fields :+ Hint(value)
          case 'P' => fields :+ Position(value.toInt)
          case 'p' => fields :+ InternalPosition(value.toInt)
          case 'q' => fields :+ InternalQuery(value)
          case 'W' => fields :+ Where(value.split('\n').toVector)
          case 's' => fields :+ Schema(value)
          case 't' => fields :+ Table(value)
          case 'c' => fields :+ Column(value)
          case 'd' => fields :+ DataType(value)
          case 'n' => fields :+ Constraint(value)
          case 'F' => fields :+ File(value)
          case 'L' => fields :+ Line(value.toInt)
          case 'R' => fields :+ Routine(value)
          case _ => fields // Ignore, per documentation recommendation
        }
      }

  }

  sealed abstract class DecoderException(msg: String) extends RuntimeException(msg)

  class MessageTooLongException(typ: Byte, length: Int, limit: Int)
    extends DecoderException(s"Message type ${Integer.toHexString(typ)} with length $length exceeds maximum of $limit bytes")

  class UnsupportedMessageTypeException(typ: Byte)
    extends DecoderException(s"Message type ${Integer.toHexString(typ)} is not supported")

  class UnsupportedAuthenticationMethodException(method: Int)
    extends DecoderException(s"Authentication method $method is not supported")

  class UnsupportedSSLReplyException(typ: Byte)
    extends DecoderException(s"SSL reply ${Integer.toHexString(typ)} is not supported")

  class UnsupportedFormatTypeException(typ: Short)
    extends DecoderException(s"Format type ${Integer.toHexString(typ)} is not supported")

  class TextOnlyCopyFormatException(columns: Iterable[Int])
    extends DecoderException(s"Text COPY format does not allow binary column types in columns ${columns.mkString(", ")}")

  class UnsupportedTransactionStatusException(typ: Byte)
    extends DecoderException(s"Transaction status ${Integer.toHexString(typ)} is not supported")

  class UnsupportedParameterValueException(value: String)
    extends DecoderException(s"Parameter value '$value' is not supported")

}