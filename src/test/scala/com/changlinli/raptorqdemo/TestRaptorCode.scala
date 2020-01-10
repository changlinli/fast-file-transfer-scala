package com.changlinli.raptorqdemo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ArraySeq

class TestRaptorCode extends AnyFlatSpec with Matchers {
  "A basic sanity test" should "work" in {
    val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))
    val (fecParameters, encodedBytes) = RaptorQEncoder.encode(myBytes, 2, 5)
    val loseALotOfEncodedBytes = encodedBytes.dropWhile(packet => packet.encodingSymbolID() % 2 == 0)
    val result = BatchRaptorQDecoder.batchDecode(loseALotOfEncodedBytes.take(1000).toList, fecParameters)
    result should be (myBytes)
  }

}
