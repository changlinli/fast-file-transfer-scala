package com.changlinli.raptorqdemo

import java.{lang, util}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import cats.Monad
import cats.effect.{Blocker, Concurrent, ContextShift, ExitCode, IO, IOApp, Resource, Sync}
import cats.implicits._
import fs2.Chunk
import fs2.io.udp.{Packet, Socket, SocketGroup}
import net.fec.openrq.decoder.DataDecoder
import net.fec.openrq.parameters.{FECParameters, ParameterChecker}
import net.fec.openrq.{EncodingPacket, OpenRQ}

import scala.collection.immutable.ArraySeq
import scala.collection.immutable.ArraySeq.ofByte
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable, ParSeq}
import scala.language.higherKinds

sealed trait BatchDecodeErrors
final case class ProbabilisticDecodeFailure() extends BatchDecodeErrors
final case class MalformedPackets() extends BatchDecodeErrors

object ArraySeqUtils {
  def unsafeToByteArray(immutableByteArray: ArraySeq[Byte]): Array[Byte] = immutableByteArray match {
    case backedByByteArray: ofByte => backedByByteArray.unsafeArray
    case backedByOtherArray =>
      val resultArray = Array.fill[Byte](backedByOtherArray.length)(0)
      //noinspection ScalaUnusedExpression
      backedByOtherArray.copyToArray(resultArray)
      resultArray
  }

  def readFromFile(fileName: String): IO[ArraySeq[Byte]] = IO{
    ArraySeq.unsafeWrapArray(Files.readAllBytes(Paths.get(fileName)))
  }

  def writeToFile(fileName: String, bytes: Array[Byte]): IO[Unit] = IO {
    Files.write(Paths.get(fileName), bytes)
  }

  def writeToFile(fileName: String, bytes: ArraySeq[Byte]): IO[Unit] = IO {
    Files.write(Paths.get(fileName), unsafeToByteArray(bytes))
  }
}

object RaptorQEncoder {
  def encodeAsSingleBlock(data: ArraySeq[Byte]): (FECParameters, LazyList[EncodingPacket]) = {
    val result = encode(data, 10, 1)
    result.copy(_2 = LazyList.from(result._2))
  }

  def encode(data: ArraySeq[Byte], symbolSize: Int, numberOfSourceBlocks: Int): (FECParameters, LazyList[EncodingPacket]) = {
    val fecParameters = FECParameters.newParameters(data.length, symbolSize, numberOfSourceBlocks)
    val topLevelEncoder = OpenRQ.newEncoder(
      ArraySeqUtils.unsafeToByteArray(data),
      fecParameters
    )
    val sourceBlockEncoders = topLevelEncoder.sourceBlockIterable().asScala.toList
    val sourceBlockToEncoder = sourceBlockEncoders
      .map{encoder =>
        val maximumNumberOfRepairPackets = ParameterChecker.numRepairSymbolsPerBlock(encoder.numberOfSourceSymbols())
        val repairPackets = encoder
          .repairPacketsIterable(maximumNumberOfRepairPackets)
          .asScala
          .|>(x =>  LazyList.from(x))
        val sourcePackets = encoder
          .sourcePacketsIterable()
          .asScala
          .|>(x => LazyList.from(x))
        sourcePackets.appendedAll(repairPackets)
      }
      .zipWithIndex
      .map(_.swap)
      .toMap
    val allPackets = LazyList.unfold((0, sourceBlockToEncoder)){
      case (sourceBlockNumber, usedSourceBlockEncoders) =>
        val listOfPackets = usedSourceBlockEncoders
          .getOrElse(sourceBlockNumber, throw new Exception("Blahblah"))
        val (packetsToAdd, newListOfPackets) = listOfPackets.splitAt(2)
        val newSourceBlockEncoderMap = usedSourceBlockEncoders + (sourceBlockNumber -> newListOfPackets)
        val newSourceBlockNumber = (sourceBlockNumber + 1) % numberOfSourceBlocks
        val newState = (newSourceBlockNumber, newSourceBlockEncoderMap)
        Some((packetsToAdd, newState))
    }
    (fecParameters, allPackets.flatten)
  }

  def encodeAsSingleBlockA(data: ArraySeq[Byte], symbolSize: Int): (FECParameters, Iterator[EncodingPacket]) = {
    val fecParameters = FECParameters.newParameters(data.length, symbolSize, 1)
    val topLevelEncoder = OpenRQ.newEncoder(
      ArraySeqUtils.unsafeToByteArray(data),
      fecParameters
    )
    val sourceEncoder = topLevelEncoder.sourceBlock(0)
    val maximumNumberOfRepairPackets = ParameterChecker.numRepairSymbolsPerBlock(sourceEncoder.numberOfSourceSymbols())
    val sourcePackets = topLevelEncoder.sourceBlock(0).sourcePacketsIterable()
    val repairPackets = topLevelEncoder.sourceBlock(0).repairPacketsIterable(maximumNumberOfRepairPackets)
    val sourcePacketIterator = sourcePackets.iterator()
    val repairPacketIterator = repairPackets.iterator()
    val iterator = new Iterator[EncodingPacket] {
      override def hasNext: Boolean =
        sourcePacketIterator.hasNext || repairPacketIterator.hasNext

      override def next(): EncodingPacket = {
        val result = if (sourcePackets.iterator().hasNext) {
          sourcePacketIterator.next()
        } else {
          repairPacketIterator.next()
        }
        result
      }
    }
    (fecParameters, iterator)
  }


  private def takeFromJavaIterable[A](n: Int, javaIterable: java.lang.Iterable[A]): mutable.Buffer[A] = {
    var numberLeft = n
    val buffer = mutable.Buffer.empty[A]
    while (javaIterable.iterator().hasNext && numberLeft > 0) {
      buffer.append(javaIterable.iterator().next())
      numberLeft = numberLeft - 1
    }
    buffer
  }

  def encodeUnordered[F[_] : Concurrent](data: ArraySeq[Byte], symbolSize: Int, numberOfSourceBlocks: Int): (FECParameters, fs2.Stream[F, EncodingPacket]) = {
    val fecParameters = FECParameters.newParameters(data.length, symbolSize, numberOfSourceBlocks)
    val topLevelEncoder = OpenRQ.newEncoder(
      ArraySeqUtils.unsafeToByteArray(data),
      fecParameters
    )
    val sourceBlockEncoders = topLevelEncoder.sourceBlockIterable().asScala.toList
    val allIterators = sourceBlockEncoders.map{
      encoder =>
        val maximumNumberOfRepairPackets = ParameterChecker.numRepairSymbolsPerBlock(encoder.numberOfSourceSymbols())
        val repairPackets = encoder
          .repairPacketsIterable(maximumNumberOfRepairPackets)
        val sourcePackets = encoder
          .sourcePacketsIterable()
        val sourceStream = fs2.Stream.fromIterator[F](sourcePackets.asScala.iterator).chunkN(100)
        val repairStream = fs2.Stream.fromIterator[F](repairPackets.asScala.iterator).chunkN(100)
        sourceStream ++ repairStream
    }

    val stream = fs2.Stream
      .fromIterator[F](allIterators.iterator)
      .parJoinUnbounded
      .flatMap(chunk => fs2.Stream.fromIterator[F](chunk.iterator))

    (fecParameters, stream)
  }

}

object BatchRaptorQDecoder {
  def batchDecode(allPackets: List[EncodingPacket], fecParameters: FECParameters): ArraySeq[Byte] = {
    val topLevelDecoder = OpenRQ.newDecoder(fecParameters, 5)
    allPackets.foreach{
      packet =>
        topLevelDecoder.sourceBlock(packet.sourceBlockNumber()).putEncodingPacket(packet)
    }
    if (topLevelDecoder.isDataDecoded) {
      ArraySeq.unsafeWrapArray(topLevelDecoder.dataArray())
    } else {
      // FIXME
      throw new Exception("waejriaowejroiaweor")
    }
  }

  def feedSinglePacket(packet: EncodingPacket, fecParameters: FECParameters, decoder: DataDecoder): Unit = {
    decoder.sourceBlock(packet.sourceBlockNumber()).putEncodingPacket(packet)
  }

  def feedPackets(allPackets: Iterable[EncodingPacket], fecParameters: FECParameters, decoder: DataDecoder): Unit = {
    allPackets.foreach{
      packet =>
        decoder.sourceBlock(packet.sourceBlockNumber()).putEncodingPacket(packet)
    }
  }
}

object UdpProcessing {
  def sendAsUdp(data: ArraySeq[Byte]): (FECParameters, LazyList[Packet]) = {
    val (fecParameters, encodingPackets) = RaptorQEncoder.encodeAsSingleBlock(data)
    val udpPackets = encodingPackets.map(
      encodingPacket =>
        Packet(new InetSocketAddress(8011), Chunk.bytes(encodingPacket.asArray()))
    )
    (fecParameters, udpPackets)
  }

  def fromUdpPacket(fecParameters: FECParameters, udpPacket: Packet): EncodingPacket = {
    val decoder = OpenRQ.newDecoder(fecParameters, 5)
    decoder
      .parsePacket(udpPacket.bytes.toByteBuffer, true)
      // FIXME
      .value()
  }

  def fromUdp(fecParameters: FECParameters, udpPackets: LazyList[Packet]): LazyList[EncodingPacket] = {
    val decoder = OpenRQ.newDecoder(fecParameters, 5)
    udpPackets.map{
      udpPacket =>
        decoder
          .parsePacket(udpPacket.bytes.toByteBuffer, true)
          // FIXME
          .value()
    }
  }
}

object GlobalResources {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)

  def blockerAndExecutorResource[F[_] : Sync]: Resource[F, (Blocker, ExecutorService)] =
    Resource.make{
      Sync[F].delay{
        val threadPool = Executors.newCachedThreadPool()
        val blocker = Blocker.liftExecutionContext(ExecutionContext.fromExecutorService(threadPool))
        (blocker, threadPool)
      }
    }{case (_, executorService) => Sync[F].delay(executorService.shutdown())}

  def blockerResource[F[_] : Sync]: Resource[F, Blocker] = blockerAndExecutorResource[F].map(_._1)

  def socketResourceByIpAddress[F[_] : Concurrent : ContextShift](ipAddress: String, port: Int): Resource[F, Socket[F]] = blockerResource
    .flatMap(blocker => SocketGroup[F](blocker))
    .flatMap(socketGroup => socketGroup.open[F](new InetSocketAddress(InetAddress.getByName(ipAddress), port)))

  def socketResourceLocalhost[F[_] : Concurrent : ContextShift](port: Int): Resource[F, Socket[F]] = blockerResource
    .flatMap(blocker => SocketGroup[F](blocker))
    .flatMap(socketGroup => socketGroup.open[F](new InetSocketAddress(port)))

}

object UdpClient {
  val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))

  val (fecParameters1, udpPackets) = UdpProcessing.sendAsUdp(myBytes)

  def run[F[_] : Concurrent : ContextShift]: F[Unit] = {
    GlobalResources
      .socketResourceLocalhost[F](8011)
      .use{
        (socket: Socket[F]) =>
          val writeOutDummyPacket = socket
            .write(Packet(new InetSocketAddress(InetAddress.getByName("178.62.26.117"), 8012), Chunk.bytes(Array[Byte](1, 2, 3))))
            .*>(Sync[F].delay(println("Sent a dummy packet!")))
          val receivePackets = socket
            .reads()
            .take(100)
            .concurrently(fs2.Stream.eval(writeOutDummyPacket))
            .evalTap(packet => Sync[F].delay(println(s"Packet: $packet")))
            .map(UdpProcessing.fromUdpPacket(fecParameters1, _))
            .compile
            .toList
            .map(encodingPackets => BatchRaptorQDecoder.batchDecode(encodingPackets, fecParameters1))
            .flatMap(bytes => Sync[F].delay(println(s"Our bytes were: $bytes")))
//          writeOutDummyPacket.*>(receivePackets)
          receivePackets
      }
  }
}

object UdpServer {

  val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))

  val (fecParameters1, udpPackets) = UdpProcessing.sendAsUdp(myBytes)

  def writeOutPackets[F[_] : Sync](socket: Socket[F], address: InetSocketAddress): F[Unit] = {
    udpPackets
      .map(packet => packet.copy(remote = address))
      .take(1000)
      .flatMap{
        packet =>
          val firstByte: Byte = packet.bytes.last.getOrElse(0)
          if (firstByte % 3 == 0) {
            LazyList(packet)
          } else {
            LazyList.empty
          }
      }
      .toList
      .traverse_(udpPacket => Sync[F].delay(println(s"Writing out this UDP packet: $udpPacket"))*>(socket.write(udpPacket, Some(FiniteDuration(1, TimeUnit.SECONDS)))))
  }

  def respondToIncomingPacket[F[_] : Sync](udpPacket: Packet, socket: Socket[F]): F[Unit] = {
    for {
      _ <- Sync[F].delay(println(s"Received a packet: $udpPacket"))
      _ <- writeOutPackets[F](socket, udpPacket.remote)
    } yield ()
  }

  def run[F[_] : Concurrent: ContextShift]: F[Unit] = {
    GlobalResources.socketResourceLocalhost[F](8012)
      .flatMap(readSocket => GlobalResources.socketResourceLocalhost[F](8013).map((readSocket, _)))
      .use{
        case (readSocket: Socket[F], writeSocket: Socket[F]) =>
          readSocket
            .reads()
            .evalTap(packet => Sync[F].delay(println(s"Received a packet: $packet"))*>(respondToIncomingPacket(packet, readSocket)))
            .compile
            .drain
      }
  }
}

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))
    val (fecParameters, encodedBytes) = RaptorQEncoder.encode(myBytes, 2, 5)
    val loseALotOfEncodedBytes = encodedBytes.dropWhile(packet => packet.encodingSymbolID() % 2 == 0)
    val result = BatchRaptorQDecoder.batchDecode(loseALotOfEncodedBytes.take(1000).toList, fecParameters)
    println(s"Hello world!: $result")

    val action = if (args(1) == "client") {
      UdpClient.run[IO]
    } else if (args(1) == "server") {
      UdpServer.run[IO]
    } else {
      IO{
        Thread.sleep(20000)
        val myBytes = ArraySeqUtils.readFromFile("Track 10.wav").unsafeRunSync()
        val (fecParameters, encodedBytes) = RaptorQEncoder.encodeUnordered[IO](myBytes, 10000, 20)
        val loseALotOfEncodedBytes = encodedBytes.dropWhile(packet => packet.encodingSymbolID() % 2 == 0).take(100000)
        val loseALotOfEncodedBytesForced = loseALotOfEncodedBytes.take(myBytes.length + 200).compile.drain.unsafeRunSync()
        println("BEGINNING DECODE!")
      }
    }
    for {
      _ <- action
      _ <- IO(println("We're done with our action"))
    } yield ExitCode.Success
  }
}
