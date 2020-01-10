package com.changlinli.raptorqdemo

import java.net.InetSocketAddress
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import cats.effect.{Blocker, ExitCode, IO, IOApp, Resource}
import cats.implicits._
import fs2.Chunk
import fs2.io.udp.{Packet, Socket, SocketGroup}
import net.fec.openrq.parameters.FECParameters
import net.fec.openrq.{EncodingPacket, OpenRQ}

import scala.collection.immutable.ArraySeq
import scala.collection.immutable.ArraySeq.ofByte
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

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
}

object RaptorQEncoder {
  def encodeAsSingleBlock(data: ArraySeq[Byte]): (FECParameters, LazyList[EncodingPacket]) = {
    val fecParameters = FECParameters.newParameters(data.length, 10, 1)
    val encoder = OpenRQ.newEncoder(
      ArraySeqUtils.unsafeToByteArray(data),
      fecParameters
    ).sourceBlock(0)
    val numberOfSourceSymbols = data.length / 1000 + 1
    val maximumNumberOfRepairPackets = 10000
    val packets = LazyList
      .from(encoder.sourcePacketsIterable().asScala)
      .appendedAll(encoder.repairPacketsIterable(maximumNumberOfRepairPackets).asScala)
    (fecParameters, packets)
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
        val repairPackets = encoder
          .repairPacketsIterable(10000)
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
}

object BatchRaptorQDecoder {
  def batchDecode(allPackets: List[EncodingPacket], fecParameters: FECParameters): ArraySeq[Byte] = {
    val numberOfSourceBlocks = fecParameters.numberOfSourceBlocks
    val topLevelDecoder = OpenRQ.newDecoder(fecParameters, 5)
    val decoders = Range(0, numberOfSourceBlocks)
      .map {
        sourceBlock =>
          val decoder = topLevelDecoder.sourceBlock(sourceBlock)
          sourceBlock -> decoder
      }
      .toMap
    allPackets.foreach(packet => decoders.get(packet.sourceBlockNumber()).map(decoder => decoder.putEncodingPacket(packet)))
    if (topLevelDecoder.isDataDecoded) {
      ArraySeq.unsafeWrapArray(topLevelDecoder.dataArray())
    } else {
      // FIXME
      throw new Exception("waejriaowejroiaweor")
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

object UdpServer {

  def listenToSocket(socket: Socket[IO]) = {
    socket.reads()
  }
}

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))
    val (fecParameters, encodedBytes) = RaptorQEncoder.encode(myBytes, 2, 5)
    val loseALotOfEncodedBytes = encodedBytes.dropWhile(packet => packet.encodingSymbolID() % 2 == 0)
    val result = BatchRaptorQDecoder.batchDecode(loseALotOfEncodedBytes.take(1000).toList, fecParameters)
    println(s"Hello world!: $result")

    val blockerAndExecutorResource: Resource[IO, (Blocker, ExecutorService)] =
      Resource.make{
        IO{
          val threadPool = Executors.newCachedThreadPool()
          val blocker = Blocker.liftExecutionContext(ExecutionContext.fromExecutorService(threadPool))
          (blocker, threadPool)
        }
      }{case (_, executorService) => IO(executorService.shutdown())}

    val blockerResource: Resource[IO, Blocker] = blockerAndExecutorResource.map(_._1)

    val (fecParameters1, udpPackets) = UdpProcessing.sendAsUdp(myBytes)

    val action = if (args(1) == "r") {
      blockerResource
        .flatMap(blocker => SocketGroup[IO](blocker))
        .flatMap(socketGroup => socketGroup.open(new InetSocketAddress(8011)))
        .use{
          socket =>
            socket
              .reads()
              .take(100)
              .evalTap(packet => IO(println(s"Packet: $packet")))
              .map(UdpProcessing.fromUdpPacket(fecParameters1, _))
              .compile
              .toList
              .map(encodingPackets => BatchRaptorQDecoder.batchDecode(encodingPackets, fecParameters1))
              .flatMap(bytes => IO(println(s"Our bytes were: $bytes")))
        }
    } else {
      println(s"Our arg was ${args(1)}")
      blockerResource
        .flatMap(blocker => SocketGroup[IO](blocker))
        .flatMap(socketGroup => socketGroup.open(new InetSocketAddress(8012)))
        .use{
          (socket: Socket[IO]) =>
            udpPackets
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
              .traverse(udpPacket => socket.write(udpPacket, Some(FiniteDuration(1, TimeUnit.SECONDS))))
        }
    }
    for {
      _ <- action
      _ <- IO(println("We're done with our action"))
    } yield ExitCode.Success
  }
}
