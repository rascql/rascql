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

package rascql.postgresql.stream

import scala.concurrent.duration._
import akka.stream.scaladsl._
import akka.stream.testkit._
import rascql.postgresql.protocol._

/**
 * Tests for [[QueryExecution]].
 *
 * @author Philip L. McMahon
 */
class QueryExecutionSpec extends StreamSpec {

  "A send query stage" should {

    import PreparedStatement.{Unnamed => Stmt}
    import Portal.{Unnamed => Ptl}

    val stage = Flow[SendQuery].transform(() => new SendQueryStage)

    "convert a simple query" in {
      val src = TestPublisher.manualProbe[SendQuery]()
      val sink = TestSubscriber.manualProbe[FrontendMessage]()

      Source(src).via(stage).runWith(Sink(sink))

      val pub = src.expectSubscription()
      val sub = sink.expectSubscription()
      sub.request(3)
      pub.sendNext(SendQuery("SELECT 1"))
      sink.expectNext(Query("SELECT 1"))
      sink.expectNoMsg(100.millis)
      sub.cancel()
      pub.expectCancellation()
    }

    "convert a prepared statement" in {
      val src = TestPublisher.manualProbe[SendQuery]()
      val sink = TestSubscriber.manualProbe[FrontendMessage]()

      Source(src).via(stage).runWith(Sink(sink))

      val pub = src.expectSubscription()
      val sub = sink.expectSubscription()
      sub.request(6)
      pub.sendNext(SendQuery.Prepared("SELECT 1", Nil))
      sink.expectNext(Parse("SELECT 1", Nil, Stmt))
      sink.expectNext(Bind(Nil, Ptl, Stmt, None))
      sink.expectNext(Describe(Ptl))
      sink.expectNext(Execute(Ptl))
      sink.expectNext(Sync)
      sink.expectNoMsg(100.millis)
      sub.cancel()
      pub.expectCancellation()
    }

  }

}
