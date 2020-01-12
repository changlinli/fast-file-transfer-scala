package com.changlinli.raptorqdemo

import cats.effect.{ContextShift, IO}
import net.fec.openrq.{EncodingPacket, OpenRQ}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ArraySeq

class TestRaptorCode extends AnyFlatSpec with Matchers {
  "A basic sanity test" should "work" in {
    val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))
    val (fecParameters, encodedBytes) = RaptorQEncoder.encode(myBytes, 2, 5)
    val loseALotOfEncodedBytes = encodedBytes.filter(packet => packet.encodingSymbolID() % 2 == 0)
    val loseALotOfEncodedBytesForced = loseALotOfEncodedBytes.take(myBytes.length + 5).toList
    val result = BatchRaptorQDecoder.batchDecode(loseALotOfEncodedBytesForced, fecParameters)
    result should be (myBytes)
  }
  it should "blahblah" in {
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
    val (fecParameters, iterator) = RaptorQEncoder.encodeAsSingleBlockA(myBytes, 10000)
    println("BEGINNING DECODE!")
    val beginningTime = System.currentTimeMillis()
    val topLevelDecoder = OpenRQ.newDecoder(fecParameters, 5)
    iterator.takeWhile{
      packet =>
        BatchRaptorQDecoder.feedSinglePacket(packet, fecParameters, topLevelDecoder)
        !topLevelDecoder.isDataDecoded
    }.foreach(_ => ())
    println(s"WE FINISHED: ${topLevelDecoder.isDataDecoded}, ms: ${System.currentTimeMillis() - beginningTime}")
    println(s"Size of decoded data is ${topLevelDecoder.dataArray().size}")
    ArraySeqUtils.writeToFile("output.wav", topLevelDecoder.dataArray()).unsafeRunSync()
//    iterator.take(1000000).foreach(packet => BatchRaptorQDecoder.feedSinglePacket(packet, fecParameters, topLevelDecoder))
//    println(s"Is this decoded: ${topLevelDecoder.isDataDecoded}")
//    loseALotOfEncodedBytesForced should be ()
//    val result = BatchRaptorQDecoder.batchDecode(loseALotOfEncodedBytesForced, fecParameters)
//    result should be (myBytes)
  }

}
