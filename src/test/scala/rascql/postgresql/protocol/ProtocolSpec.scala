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
 * Tests for protocol-related types.
 *
 * @author Philip L. McMahon
 */
class ProtocolSpec extends WordSpec with Matchers {

  import scala.language.reflectiveCalls

  val encoding = Charset.forName("UTF-8")

  "A clear text password" should {

    "encode correctly" in {

      Password.ClearText("rascql") shouldEncodeTo "rascql\u0000"

    }

  }

  "A MD5 password" should {

    "encode correctly" in {

      val p = Password.MD5("rascql", "rascql", ByteString(-0x7F, 0x7F, 0x0, 0x0))
      p shouldEncodeTo "md5fcb002a445255eb020016880b46b41ed\u0000"

    }

  }

  type Encodable = { def encode(c: Charset): ByteString }

  implicit class RichEncodable(e: Encodable) {

    def shouldEncodeTo(right: String) =
      e.encode(encoding) shouldEqual ByteString(right)

    def shouldEncodeToRaw[T](right: T*)(implicit num: Integral[T]) =
      e.encode(encoding) shouldEqual ByteString(right: _*)

  }

}
