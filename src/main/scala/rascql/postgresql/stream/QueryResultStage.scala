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
 * Converts a [[BackendMessage]] stream to a [[QueryResult]], discarding any
 * unexpected or irrelevant messages.
 *
 * Note that the stream will finish when a [[ReadyForQuery]] is received,
 * since this stream of results is matched to a single query, which we now know
 * is complete.
 *
 * TODO Generate partial row result on downstream failure?
 */
private[stream] class QueryResultStage extends StatefulStage[BackendMessage, QueryResult] {

  def idle: State = new State {

    def onPush(msg: BackendMessage, ctx: Context[QueryResult]) =
      msg match {
        case RowDescription(fields) =>
          become(rowData(fields))
          ctx.pull()
        case CommandComplete(tag) =>
          ctx.push(QueryComplete(tag))
        case EmptyQueryResponse =>
          ctx.push(EmptyQuery)
        case ErrorResponse(fields) =>
          ctx.push(QueryFailed(fields))
        case _: ReadyForQuery =>
          ctx.finish()
        case _ =>
          ctx.pull() // FIXME What else do we need to handle?
      }

  }

  def rowData(fields: RowDescription.Fields): State = new State {

    var rows = Vector.empty[DataRow]

    def onPush(msg: BackendMessage, ctx: Context[QueryResult]) =
      msg match {
        case row: DataRow =>
          rows +:= row
          ctx.pull()
        case CommandComplete(tag) =>
          become(idle)
          ctx.push(QueryRowSet(tag, fields, rows))
        case ErrorResponse(fields) =>
          become(idle)
          ctx.push(QueryFailed(fields))
        case _: ReadyForQuery =>
          ctx.finish()
        case _ =>
          ctx.pull() // FIXME What else do we need to handle?
      }

  }

  def initial = idle

}
