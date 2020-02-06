package com.changlinli.raptorqdemo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.Checkers

class TestOnlineCodes extends AnyFlatSpec with Matchers with Checkers {
  "Stripping all ordinary message blocks" should "recover the original message" in {
    val bunchOfOrdinaryBlocks = Vector(
      NonterminalMessageBlock(DirectImmutableByteArrayView(1, 2, 3), 0),
      NonterminalMessageBlock(DirectImmutableByteArrayView(1, 2, 3), 1),
      TerminalMessageBlock(DirectImmutableByteArrayView(1, 2, 3), 0, 2)
    )
    val strippedResult = OnlineCodes.stripPreprocessedData(bunchOfOrdinaryBlocks, 3)
    strippedResult should be (DirectImmutableByteArrayView(1))
  }

}
