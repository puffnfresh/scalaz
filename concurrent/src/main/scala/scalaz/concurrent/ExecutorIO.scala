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
      IO(e.submit(new Runnable { def run() { io.unsafePerformIO } })).map { f =>
        ForkedIO.fromFuture(f)
      }
    }
}

case class ScheduleIO(schedule: (IO[Unit], Long, TimeUnit) => IO[ForkedIO])

object ScheduleIO {
  def fromScheduledExecutorService(e: ScheduledExecutorService) =
    ScheduleIO { (io, t, tu) =>
      IO(e.schedule(new Runnable { def run() { io.unsafePerformIO } }, t, tu)).map { f =>
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
      _ <- x.read
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
}
