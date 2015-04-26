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

import java.nio.charset.Charset
import akka.actor.ActorSystem
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
  implicit val materializer = ActorFlowMaterializer()

  var charset = Charset.forName("UTF-8")
  val bulkDecoder = BulkDecoder(charset, 16 * 1024 * 1024)

  val Array(username, password) = args

  import FlowGraph.Implicits._

  val decoder = Flow[ByteString].named("decoder").
    transform(() => DecoderStage(bulkDecoder)).
    log("Decoder")

  val encoder = Flow[FrontendMessage].named("encoder").
    log("Encoder").
    map(_.encode(charset))

  val query = Source.single(List(
    """BEGIN;
      |SELECT usename FROM pg_stat_activity;
      |COMMIT""".stripMargin
  ).map(Query.apply))

  val preparedStatement = Source.single(List(
    Parse("SELECT usename FROM pg_stat_activity WHERE usename = $1 LIMIT $2"),
    Bind(Seq(username, 1)),
    Describe(PreparedStatement.Unnamed),
    Execute(Portal.Unnamed),
    Sync
  ))

  val close = Source.single(List(Terminate))

  // Authentication flow begins with startup message and then continues until an error is received or AuthOk
  val login = Flow() { implicit b =>
    val merge = b.add(Merge[FrontendMessage](2))
    val auth = b.add(Flow[BackendMessage].named("authentication").
      transform(() => AuthenticationStage(username, password)))
    val startup = b.add(Source.single[FrontendMessage](StartupMessage(
      user = username,
      parameters = Map(
        "database" -> username,
        "application_name" -> "rascql-demo",
        "client_encoding" -> charset.name()
      )
    )))

    startup ~> merge
    auth ~> merge

    (auth.inlet, merge.out)
  }

  val flow = Flow() { implicit b =>
    val merge = b.add(Merge[FrontendMessage](2))
    val bcast = b.add(Broadcast[BackendMessage](2))
    val rfq = Flow[BackendMessage].named("ready-for-query").
      splitWhen(_.isInstanceOf[ReadyForQuery])
    val zip = b.add(Zip[Source[BackendMessage, Unit], List[FrontendMessage]]())
    val unzip = b.add(Unzip[Source[BackendMessage, Unit], List[FrontendMessage]]())

    // Zip each query chunk with the ReadyForQuery message, so messages are
    // sent when the server is ready for them.

    bcast ~> login ~> merge
    bcast ~> rfq ~> zip.in0
    query.concat(preparedStatement).concat(close) ~> zip.in1
    zip.out ~> unzip.in
    unzip.out0.flatten(FlattenStrategy.concat) ~> Sink.foreach[Any](println)
    unzip.out1.mapConcat(identity) ~> merge

    (bcast.in, merge.out)
  }

  val conn = Tcp().outgoingConnection(host = "localhost", port = 5432)

  decoder.via(flow).via(encoder).join(conn).run()

  // FIXME Disconnect and stop actor system when queries complete

}
