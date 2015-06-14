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

  def flow[In, Out, Mat](flow: Flow[In, Out, Mat])(fn: (TestPublisher.ManualProbe[In], TestSubscriber.ManualProbe[Out]) => Unit): Unit = {
    val src = TestPublisher.manualProbe[In]()
    val sink = TestSubscriber.manualProbe[Out]()

    Source(src).via(flow).runWith(Sink(sink))

    fn(src, sink)
  }

  "A send query stage" should {

    import PreparedStatement.{Unnamed => Stmt}
    import Portal.{Unnamed => Ptl}

    val stage = Flow[SendQuery].transform(() => new SendQueryStage)

    "convert a simple query" in flow(stage) { (src, sink) =>
      val pub = src.expectSubscription()
      val sub = sink.expectSubscription()
      sub.request(3)
      pub.sendNext(SendQuery("SELECT 1"))
      sink.expectNext(Query("SELECT 1"))
      sink.expectNoMsg(100.millis)
      sub.cancel()
      pub.expectCancellation()
    }

    "convert a prepared statement" in flow(stage) { (src, sink) =>
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

  "A query result stage" should {

    val stage = Flow[BackendMessage].transform(() => new QueryResultStage)

    "produce an empty result" in flow(stage) { (src, sink) =>
      val pub = src.expectSubscription()
      val sub = sink.expectSubscription()
      sub.request(2)
      pub.sendNext(EmptyQueryResponse)
      sink.expectNext(EmptyQuery)
      sink.expectNoMsg(100.millis)
      sub.cancel()
      pub.expectCancellation()
    }

  }

}
