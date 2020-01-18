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
      _ <- clientFiber.join
    } yield ()

    combinedAction.unsafeRunSync()
    //    val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))
//    val (fecParameters, encodedBytes) = RaptorQEncoder.encode(myBytes, 2, 5)
//    val filterToOnlyOneSourceBlock = encodedBytes.filter(packet => packet.sourceBlockNumber() == 1)
//    val loseALotOfEncodedBytesForced = filterToOnlyOneSourceBlock.take(myBytes.length + 50).toList
//    val result = BatchRaptorQDecoder.batchDecode(loseALotOfEncodedBytesForced, fecParameters)
//    result should be (myBytes)
  }
  "Getting a file" should "work" in {
    implicit val contextIO: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)
    val myBytes = ArraySeqUtils.readFromFile("Track 10.wav").unsafeRunSync()
//    val (fecParameters, encodedBytes) = RaptorQEncoder.encode(myBytes, 1000, 1)
//    val loseALotOfEncodedBytes = encodedBytes.dropWhile(packet => packet.encodingSymbolID() % 2 == 0).take(1000000)
//    val loseALotOfEncodedBytesForced = loseALotOfEncodedBytes.take(myBytes.length + 200).compile.drain.unsafeRunSync()
//    encodedBytes.take(100000).force
    println("BEGINNING ENCODE!")
    val (fecParameters, iterator) = RaptorQEncoder.encodeAsSingleBlockIterator(myBytes, 10000)
    println("BEGINNING DECODE!")
    val beginningTime = System.currentTimeMillis()
    val topLevelDecoder = OpenRQ.newDecoder(fecParameters, 5)
    iterator.takeWhile{
      packet =>
        !BatchRaptorQDecoder.feedSinglePacket(packet, fecParameters, topLevelDecoder)
    }.foreach(_ => ())
    println(s"WE FINISHED: ${topLevelDecoder.isDataDecoded}, ms: ${System.currentTimeMillis() - beginningTime}")
    println(s"Size of decoded data is ${topLevelDecoder.dataArray().length}")
    val (fecParameters0, stream) = RaptorQEncoder.encodeAsSingleBlockStream[IO](myBytes, 10000)
    val topLevelDecoder0 = OpenRQ.newDecoder(fecParameters0, 5)
    val beginningTimeIO = System.currentTimeMillis()
//    GlobalResources.socketResourceLocalhost[IO](8012).use{
//      socket =>
//        val inputStream = stream
//          .map(encodingPacket => Packet(new InetSocketAddress(InetAddress.getLocalHost, 8012), Chunk.bytes(encodingPacket.asArray()))).take(4000)
//          .through(socket.writes())
//        val outputStream = socket
//          .reads()
//          .map(packet => topLevelDecoder.parsePacket(packet.bytes.toBytes.values, false).value())
//          .evalMap(BatchRaptorQDecoder.feedSinglePacketSync[IO](_, fecParameters0, topLevelDecoder0))
//          .takeWhile(isDecoded => !isDecoded)
//        outputStream
//          .concurrently(inputStream)
//          .compile
//          .drain
//    }
//      .unsafeRunSync()
//    stream
//      .evalMap(BatchRaptorQDecoder.feedSinglePacketSync[IO](_, fecParameters0, topLevelDecoder0))
//      .takeWhile(isDecoded => !isDecoded)
//      .compile
//      .drain
//      .unsafeRunSync()
    println(s"WE FINISHED IO: ms: ${System.currentTimeMillis() - beginningTimeIO}")
    ArraySeqUtils.writeToFile[IO]("output.wav", topLevelDecoder.dataArray()).unsafeRunSync()
//    iterator.take(1000000).foreach(packet => BatchRaptorQDecoder.feedSinglePacket(packet, fecParameters, topLevelDecoder))
//    println(s"Is this decoded: ${topLevelDecoder.isDataDecoded}")
//    loseALotOfEncodedBytesForced should be ()
//    val result = BatchRaptorQDecoder.batchDecode(loseALotOfEncodedBytesForced, fecParameters)
//    result should be (myBytes)
  }

}
