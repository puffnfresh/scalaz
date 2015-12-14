package scalaz
package concurrent

import effect.IO

import java.util.concurrent.{ Future => JFuture, Executors, ExecutorService, TimeUnit, ScheduledExecutorService, ScheduledFuture }

case class ForkedIO(cancel: IO[Unit], get: IO[Unit])

object ForkedIO {
  def fromFuture[A](f: JFuture[A]) =
    ForkedIO(IO(f.cancel(true)), IO(f.get))
}

case class SubmitIO(submit: IO[Unit] => IO[ForkedIO])

object SubmitIO {
  def fromExecutorService(e: ExecutorService) =
    SubmitIO { io =>
      IO(e.submit(io.unsafeRunnable)).map { f =>
        ForkedIO.fromFuture(f)
      }
    }
}

case class ScheduleIO(schedule: (IO[Unit], Long, TimeUnit) => IO[ForkedIO])

object ScheduleIO {
  def fromScheduledExecutorService(e: ScheduledExecutorService) =
    ScheduleIO { (io, t, tu) =>
      IO(e.schedule(io.unsafeRunnable, t, tu)).map { f =>
        ForkedIO.fromFuture(f)
      }
    }
}

object ExecutorIO {
  def scheduledThreadPool(i: Int) =
    IO(Executors.newScheduledThreadPool(i)).map(ScheduleIO.fromScheduledExecutorService)

  def fixedThreadPool(i: Int) =
    IO(Executors.newFixedThreadPool(i)).map(SubmitIO.fromExecutorService)

  def timeoutOn(e: ScheduleIO, t: Long, tu: TimeUnit) =
    for {
      x <- MVar.newEmptyMVar[Unit]
      _ <- e.schedule(x.put(()), t, tu)
      _ <- x.take
    } yield ()

  def timeout(t: Long, tu: TimeUnit) =
    for {
      e <- scheduledThreadPool(1)
      _ <- timeoutOn(e, t, tu)
    } yield ()

  def forkIO[A](io: IO[A]): IO[ForkedIO] =
    for {
      e <- fixedThreadPool(1)
      s <- e.submit(io.map(_ => ()))
    } yield s

  def submitToMVar[A](io: IO[A], e: SubmitIO): IO[(MVar[A], ForkedIO)] =
    for {
      m <- MVar.newEmptyMVar[A]
      f <- e.submit(io.flatMap(m.put(_)))
    } yield (m, f)

  def waitAny[A](ios: NonEmptyList[IO[A]], e: SubmitIO): IO[A] =
    for {
      m <- MVar.newEmptyMVar[A]
      fs <- Traverse[NonEmptyList].traverse(ios) { io => e.submit(io.flatMap(m.put(_))) }
      a <- m.take
      _ <- Traverse[NonEmptyList].traverse_(fs)(_.cancel)
    } yield a

  def waitBoth[A, B](ioa: IO[A], iob: IO[B], e: SubmitIO): IO[(A, B)] =
    for {
      af <- submitToMVar(ioa, e)
      bf <- submitToMVar(iob, e)
      a <- af._1.take
      b <- bf._1.take
    } yield (a, b)

  def waitEither[A, B](ioa: IO[A], iob: IO[B], e: SubmitIO): IO[A \/ B] =
    waitAny(NonEmptyList(ioa.map(-\/(_)), iob.map(\/-(_))), e)
}
