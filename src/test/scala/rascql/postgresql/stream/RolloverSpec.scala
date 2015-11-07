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

import akka.stream.ClosedShape
import akka.stream.scaladsl._
import akka.stream.testkit._
import org.scalatest._

/**
 * Tests for [[Rollover]].
 *
 * @author Philip L. McMahon
 */
class RolloverSpec extends StreamSpec with WordSpecLike {

  "A rollover" must {

    import FlowGraph.Implicits._

    "switch to next input on active upstream finish" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph.fromGraph(FlowGraph.create() { implicit b =>
        val rollover = b.add(Rollover[Int](2))
        Source(List(1, 2)) ~> rollover.in
        rollover ~> Sink(c1)
        rollover ~> Sink(c2)
        ClosedShape
      }).run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub1.request(1)
      c1.expectNext(1)
      sub1.cancel()
      sub2.request(1)
      c2.expectNext(2)
      sub1.cancel()
    }

  }

}
