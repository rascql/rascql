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

import akka.stream.stage._
import rascql.postgresql.protocol._

/**
 * Converts [[SendQuery]] messages into PostgreSQL messages.
 *
 * The unnamed [[Portal]]/[[PreparedStatement]] is (re-)used for each
 * [[SendExtendedQuery]] element.
 *
 * @author Philip L. McMahon
 */
private[stream] class SendQueryStage extends StatefulStage[SendQuery, FrontendMessage] {

  import PreparedStatement.{Unnamed => prepared}
  import Portal.{Unnamed => portal}

  def initial = new StageState[SendQuery, FrontendMessage] {

    def onPush(elem: SendQuery, ctx: Context[FrontendMessage]) =
      elem match {
        case SendSimpleQuery(stmt) =>
          ctx.push(Query(stmt))
        case SendExtendedQuery(stmt, params) =>
          emit(Iterator(
            Parse(stmt, Nil, prepared),
            Bind(params, portal, prepared, None),
            Describe(portal),
            Execute(portal),
            Sync
          ), ctx)
      }

  }

}
