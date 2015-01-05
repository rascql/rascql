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
import akka.event.Logging
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import rascql.postgresql.protocol._
import rascql.postgresql.stream._

/**
 * A simple demonstration of PostgreSQL query execution.
 *
 * @author Philip L. McMahon
 */
object Demo extends App with DefaultEncoders with DefaultDecoders {

  implicit val system = ActorSystem("Example")
  implicit val materializer = FlowMaterializer()

  val log = Logging(system, "rascql-demo")
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
      _.transform(() => new AuthenticationStage(username, password))
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

  val query = Source.single(List(
    """BEGIN;
      |SELECT usename FROM pg_stat_activity;
      |COMMIT""".stripMargin
  ).map(Query.apply))

  val preparedStatement = Source.single(List(
    Parse("SELECT usename FROM pg_stat_activity WHERE usename = $1 LIMIT $2"),
    Bind(Seq(Parameter(username), Parameter(1))),
    Describe(PreparedStatement.Unnamed),
    Execute(Portal.Unnamed),
    Sync
  ))

  val close = Source.single(List(Terminate))

  val flow = Flow() { implicit b =>
    val merge = Merge[FrontendMessage]
    val bcastIn = Broadcast[BackendMessage]
    val bcastOut = Broadcast[(Source[_], List[FrontendMessage])]
    val backend = UndefinedSource[BackendMessage]
    val frontend = UndefinedSink[FrontendMessage]
    val rfq = Flow[BackendMessage].section(name("ready-for-query")) {
      _.splitWhen(_.isInstanceOf[ReadyForQuery])
    }
    val zipper = Zip[Source[_], List[FrontendMessage]]
    val unzipper = Flow[(Source[_], List[FrontendMessage])]

    // Zip each query chunk with the ReadyForQuery message, so messages are
    // sent when the server is ready for them.

    backend ~> bcastIn ~> login ~> merge ~> frontend
               bcastIn ~> rfq ~> zipper.left
    query.concat(preparedStatement).concat(close) ~> zipper.right
    zipper.out ~> bcastOut ~> unzipper.mapConcat(_._2) ~> merge
                  bcastOut ~> unzipper.map(_._1).flatten(FlattenStrategy.concat) ~> Sink.foreach[Any](println)

    backend -> frontend
  }

  val codec = Flow() { implicit b =>
    val decoder = Flow[ByteString].section(name("decoder")) {
      _.transform(() => new DecoderStage(charset, maxLength)).
        transform(() => new LoggingStage("Decoder", log))
    }
    val encoder = Flow[FrontendMessage].section(name("encoder")) {
      _.transform(() => new LoggingStage("Encoder", log)).
        map(_.encode(charset))
    }
    val inbound = UndefinedSource[ByteString]
    val outbound = UndefinedSink[ByteString]

    inbound ~> decoder ~> flow ~> encoder ~> outbound

    inbound -> outbound
  }

  val conn = StreamTcp().outgoingConnection(
    remoteAddress = new InetSocketAddress("localhost", 5432),
    connectTimeout = 1.second)

  conn.handleWith(codec)

  // FIXME Disconnect and stop actor system when queries complete

}
