package com.changlinli.raptorqdemo

import java.net.{InetAddress, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import cats.effect.{Concurrent, ContextShift, IO, Timer}
import com.changlinli.raptorqdemo.UdpClient.downloadFileAA
import com.changlinli.raptorqdemo.UdpServer.{dealWithSocketOnce, processOneElementOfServerState}
import fs2.Chunk
import fs2.io.udp.Packet
import net.fec.openrq.{EncodingPacket, OpenRQ}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ArraySeq
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class TestRaptorCode extends AnyFlatSpec with Matchers {
  implicit val contextIO: ContextShift[IO] = IO.contextShift(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4)))

  implicit val timerIO: Timer[IO] = IO.timer(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4)))

  "A basic sanity test" should "work" in {
    val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))
    val (fecParameters, encodedBytes) = RaptorQEncoder.encode(myBytes, 2, 5)
    val loseALotOfEncodedBytes = encodedBytes.filter(packet => packet.encodingSymbolID() % 2 == 0)
    val loseALotOfEncodedBytesForced = loseALotOfEncodedBytes.take(myBytes.length + 5).toList
    val result = BatchRaptorQDecoder.batchDecode(loseALotOfEncodedBytesForced, fecParameters)
    result should be (myBytes)
  }
  it should "work with dummy sockets" in {

    val clientAction = GlobalResources.blockerResource[IO]
      .flatMap(blocker => DummySocket.asResource[IO](8011).map((_, blocker)))
      .use{
        case (socket, blocker) => downloadFileAA[IO](blocker, socket)
      }

    val serverAction = GlobalResources.blockerResource[IO]
      .flatMap(blocker => DummySocket.asResource[IO](8012).map(socket => (blocker, socket)))
      .use{
        case (blocker, socket) =>
          val listeningToSocketStream = fs2.Stream.repeatEval(dealWithSocketOnce[IO](blocker, socket, UdpServer.serverState))
          listeningToSocketStream
            .evalMap(_ => Concurrent[IO].start(processOneElementOfServerState[IO](UdpServer.serverState, socket)))
            // Purely for testing to make sure that we actually terminate the test
            .evalMap(_ => IO(UdpServer.serverState.get().currentRequestsBeingProcessed.isEmpty))
            .takeWhile(x => !x)
            .compile
            .drain
      }

    val combinedAction = for {
      clientFiber <- clientAction.start
      _ <- IO.sleep(FiniteDuration(1, TimeUnit.SECONDS))
      serverFiber <- serverAction.start
      _ <- serverFiber.join
      fileDownloaded <- clientFiber.join
    } yield fileDownloaded

    combinedAction.unsafeRunSync().length should be (36510210)
  }

}
