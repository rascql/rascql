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

import java.net.InetSocketAddress
import java.nio.charset.Charset
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import rascql.postgresql.protocol._
import rascql.postgresql.stream._

/**
 * A simple demonstration of PostgreSQL query execution.
 *
 * @author Philip L. McMahon
 */
object Demo extends App {

  implicit val system = ActorSystem("Example")
  implicit val materializer = FlowMaterializer()

  val charset = Charset.forName("UTF-8")
  val maxLength = 16 * 1024 * 1024

  val Array(username, password) = args

  import OperationAttributes._
  import FlowGraphImplicits._

  // Authentication flow begins with startup message and then continues until an error is received or AuthOk
  val login = Flow() { implicit b =>
    val in = UndefinedSource[BackendMessage]
    val out = UndefinedSink[FrontendMessage]
    val merge = Merge[FrontendMessage]
    val auth = Flow[BackendMessage].section(name("authenticator")) {
      _.transform(() => new Authenticator(username, password))
    }
    val startup = Source.single(StartupMessage(
      user = username,
      parameters = Map(
        "database" -> username,
        "application_name" -> "rascql-demo",
        "client_encoding" -> charset.name()
      )
    ))

    startup ~> merge
    in ~> auth ~> merge ~> out

    in -> out
  }

  val queries = Source[FrontendMessage](List(
    "BEGIN;SELECT usename FROM pg_stat_activity",
    "commit"
  ).map(Query.apply))

  val flow = Flow() { implicit b =>
    val concat = Concat[FrontendMessage]
    val backend = UndefinedSource[BackendMessage]
    val frontend = UndefinedSink[FrontendMessage]

    backend ~> login ~> concat.first
    queries ~> concat.second
    concat.out ~> frontend

    backend -> frontend
  }

  val codec = Flow() { implicit b =>
    val decoder = Flow[ByteString].section(name("decoder")) {
      _.transform(() => new MessageDecoder(charset, maxLength))
    }
    val encoder = Flow[FrontendMessage].section(name("encoder")) {
      _.map(_.encode(charset))
    }
    val bcastIn = Broadcast[BackendMessage](name("in"))
    val concat = Concat[FrontendMessage]
    val bcastOut = Broadcast[FrontendMessage](name("out"))
    val inbound = UndefinedSource[ByteString]
    val outbound = UndefinedSink[ByteString]
    val disconnect = Source.single(Terminate)

    inbound ~> decoder ~> bcastIn ~> flow ~> concat.first
    disconnect ~> concat.second
    concat.out ~> bcastOut ~> encoder ~> outbound
    bcastIn ~> Sink.foreach[Any](m => println(s"Inbound received $m"))
    bcastIn ~> Sink.onComplete(r => println(s"Inbound complete $r"))
    bcastOut ~> Sink.foreach[Any](m => println(s"Outbound sent $m"))
    bcastOut ~> Sink.onComplete(r => println(s"Outbound complete $r"))

    inbound -> outbound
  }

  val conn = StreamTcp().outgoingConnection(
    remoteAddress = new InetSocketAddress("localhost", 5432),
    connectTimeout = 1.second)

  conn.handleWith(codec)

  // FIXME Disconnect and stop actor system when queries complete

}
