Rascql: Reactive, Asynchronous Scala Query Library
===

An asynchronous Scala client for PostgreSQL built on top of [Akka Streams](http://akka.io/).

Following the example of Akka HTTP (and others), re-usable stream components are provided atop an immutable `case class`-based domain model to meet the goal of supreme composability.

Inspired by [postgresql-async](https://github.com/mauricio/postgresql-async).

The demo can be started using SBT:

    $ sbt
    sbt> test:run pguser pgpass

This project is a work-in-progress. Feedback and patches are welcome.
