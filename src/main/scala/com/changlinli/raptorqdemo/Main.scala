package com.changlinli.raptorqdemo

import java.nio.ByteBuffer

import net.fec.openrq.{EncodingPacket, OpenRQ}
import net.fec.openrq.parameters.{FECParameters, ParameterChecker}
import net.fec.openrq.util.io.ByteBuffers

import scala.collection.immutable.ArraySeq
import scala.collection.immutable.ArraySeq.ofByte
import scala.jdk.CollectionConverters._

sealed trait DecodeEncodingPacketResult
final case class FinishedDecoding(data: Array[Byte]) extends DecodeEncodingPacketResult
final case class NotYetDone(newDecoderState: RaptorQDecoder) extends DecodeEncodingPacketResult

final case class RaptorQDecoder(
  fecParameters: FECParameters,
  blocks: List[EncodingPacket]
) {
  def decodeCurrentElements(encodingPacket: EncodingPacket): DecodeEncodingPacketResult = {
    ???
  }
}

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

object BulkRaptorQEncoder {
  def splitUpDataAsSingleBlock(data: ArraySeq[Byte]): (FECParameters, LazyList[EncodingPacket]) = {
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

  def splitUpData(data: ArraySeq[Byte], symbolSize: Int, numberOfSourceBlocks: Int): (FECParameters, LazyList[EncodingPacket]) = {
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

object BulkRaptorQDecoder {
  def bulkDecode(allPackets: List[EncodingPacket], fecParameters: FECParameters): ArraySeq[Byte] = {
    val numberOfSourceBlocks = fecParameters.numberOfSourceBlocks
    val topLevelDecoder = OpenRQ.newDecoder(fecParameters, 5)
    val decoders = Range(0, numberOfSourceBlocks)
      .map{
        sourceBlock =>
          val decoder = topLevelDecoder.sourceBlock(sourceBlock)
          sourceBlock -> decoder
      }
      .toMap
    allPackets.foreach(packet => decoders.get(packet.sourceBlockNumber()).map(decoder => decoder.putEncodingPacket(packet)))
    if (topLevelDecoder.isDataDecoded) {
      ArraySeq.unsafeWrapArray(topLevelDecoder.dataArray())
    } else {
      throw new Exception("waejriaowejroiaweor")
    }
  }
}

final case class BulkRaptorQDecoder(
  fecParameters: FECParameters,
)

final case class RaptorQEncoder() {
  def generatePacket: (Array[Byte], RaptorQEncoder) = ???
}

object RaptorQDecoder {
  def newDecoder(fecParameters: FECParameters): RaptorQDecoder = new RaptorQDecoder(fecParameters, List.empty)
}

object Main {
  def main(args: Array[String]): Unit = {
    val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))
    val (fecParameters, encodedBytes) = BulkRaptorQEncoder.splitUpData(myBytes, 2, 5)
    val loseALotOfEncodedBytes = encodedBytes.dropWhile(packet => packet.encodingSymbolID() % 2 == 0)
    val result = BulkRaptorQDecoder.bulkDecode(loseALotOfEncodedBytes.take(1000).toList, fecParameters)
    println(s"Hello world!: $result")
  }
}
