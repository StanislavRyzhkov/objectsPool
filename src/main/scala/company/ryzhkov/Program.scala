package company.ryzhkov

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, ExitCode, IO, IOApp, Resource}
import cats.syntax.all._

import scala.collection.immutable.Queue
import scala.util.Random

class FixedSizedPool[A] private (stateRef: Ref[IO, FixedSizedPool.State[A]])(implicit f: Concurrent[IO]) {
  private def get: IO[A] = {
    Deferred[IO, A].flatMap { deferred =>
      stateRef.modify { state =>
        state.available.dequeueOption match {
          case Some((value, newQueue)) =>
            val newState = state.copy(available = newQueue)
            (newState, Right(value))
          case None =>
            val newState = state.copy(waiting = state.waiting.enqueue(deferred))
            (newState, Left(deferred))
        }
      }
    }.flatMap {
      case Left(value) => value.get
      case Right(value) => IO.pure(value)
    }
  }

  private def put(obj: A): IO[Unit] = {
    stateRef.modify { state =>
      state.waiting.dequeueOption match {
        case Some((deferred, newQueue)) =>
          val newState = state.copy(waiting = newQueue)
          (newState, Some(deferred))
        case None =>
          val newState = state.copy(available = state.available.enqueue(obj))
          (newState, None)
      }
    }.flatMap {
      case Some(deferred) => deferred.complete(obj).void
      case None => IO.unit
    }
  }

  def use[B](f: A => IO[B]): IO[B] =
    get.flatMap { obj => f(obj).guarantee(put(obj)) }
}

object FixedSizedPool {
  private case class State[A](available: Queue[A], waiting: Queue[Deferred[IO, A]])

  def apply[A](size: Int, factory: Resource[IO, A])(implicit f: Concurrent[IO]): Resource[IO, FixedSizedPool[A]] = {
    val x: List[Resource[IO, A]] = List.fill(size)(factory)
    val x2: Resource[IO, List[A]] = x.sequence
    x2.flatMap { available =>
      val initialState = State[A](available = available.to(Queue), waiting = Queue.empty)

      Resource.eval(
        Ref[IO].of(initialState).map { stateRef =>
          new FixedSizedPool[A](stateRef)
        }
      )
    }
  }
}

object Program extends IOApp {

  class Connection(val id: Int)

  def acquire: IO[Connection] = IO {
    val id = Random.between(0, 100)
    println(s"Connection $id created")
    new Connection(id)
  }

  def release(connection: Connection): IO[Unit] = IO {
    println(s"Connection ${connection.id} closing")
  }

  val connectionResource: Resource[IO, Connection] = Resource.make(acquire)(release)

  def handle(connection: Connection): IO[Int] = IO {
    println(s"Handling connection ${connection.id}")
    connection.id
  }

  val res = FixedSizedPool[Connection](5, connectionResource).use { poll =>
    poll.use { conn =>
      handle(conn)
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {
    res.as(ExitCode.Success)
  }
}
