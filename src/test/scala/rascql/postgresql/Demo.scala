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

  val Array(username, password) = args

  import FlowGraph.Implicits._

  val queries = Source(List(
    SendQuery(
      """BEGIN;
        |SELECT usename FROM pg_stat_activity;
        |COMMIT""".stripMargin
    ),
    SendQuery.Prepared(
      "SELECT usename FROM pg_stat_activity WHERE usename = $1 LIMIT $2",
      username,
      1
    )
  ))

  val startup = Startup(username, password, Map(
    "database" -> username,
    "application_name" -> "rascql-demo"
  ))

  val flow = Flow() { implicit b =>
    val concat = b.add(Concat[FrontendMessage]())
    val rollover = b.add(Rollover[BackendMessage]())
    val rfq = Flow[BackendMessage].named("ready-for-query").
      splitWhen(_.isInstanceOf[ReadyForQuery])
    val zip = b.add(Zip[Source[BackendMessage, Unit], SendQuery]())
    val unzip = b.add(Unzip[Source[BackendMessage, Unit], SendQuery]())
    val stmts = b.add(Flow[SendQuery].transform(() => new SendQueryStage))

    // Zip each query chunk with the ReadyForQuery message, so messages are
    // sent when the server is ready for them.

    rollover ~> startup ~> concat
    rollover ~> rfq ~> zip.in0
    queries ~> zip.in1
    zip.out ~> unzip.in
    unzip.out0.flatten(FlattenStrategy.concat) ~> Sink.foreach[Any](println)
    unzip.out1 ~> stmts ~> concat

    (rollover.in, concat.out)
  }

  val conn = Tcp().outgoingConnection(host = "localhost", port = 5432)

  flow.join(Codec(charset)).join(conn).run()

  // FIXME Disconnect and stop actor system when queries complete

}
