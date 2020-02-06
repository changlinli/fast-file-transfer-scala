package com.changlinli.raptorqdemo

import java.util

import cats.data.{NonEmptyList, NonEmptyVector}
import cats.kernel.Monoid
import org.apache.commons.math3.distribution.{EnumeratedDistribution, EnumeratedIntegerDistribution}
import org.apache.commons.math3.random.{RandomGenerator, Well19937c}

import scala.collection.immutable.{ArraySeq, SortedMap, SortedSet}

final case class NonEmptySet[A](toSet: Set[A]) {
  def length: Int = toSet.size

  def size: Int = toSet.size

  def foreach(f: A => Unit): Unit = {
    toSet.foreach(f)
  }
}

object NonEmptySet {
  def fromSet[A](set: Set[A]): Option[NonEmptySet[A]] =
    if (set.nonEmpty) Some(NonEmptySet(set)) else None

  def fromSetUnsafe[A](set: Set[A]): NonEmptySet[A] = NonEmptySet(set)
}

sealed trait ImmutableByteArrayView {
  def unsafeUnderlyingArray: Array[Byte]

  def offset: Int

  def length: Int

  def get(index: Int): Option[Byte] = if (index < length && index >= 0) {
    Some(unsafeGet(index))
  } else {
    None
  }

  def unsafeGet(index: Int): Byte

  def copyToArray(): Array[Byte] = {
    val result = new Array[Byte](length)
    var i = 0
    while(i < length) {
      result(i) = unsafeGet(i)
      i += 1
    }
    result
  }

  def unsafeCopyIntoArray(offsetAtCopy: Int, into: Array[Byte]): Unit = {
    var i = 0
    while (i < length) {
      into(i + offsetAtCopy) = unsafeGet(i)
      i += 1
    }
  }
}

final case class OffsetImmutableByteArrayView(unsafeUnderlyingArray: Array[Byte], offset: Int, length: Int) extends ImmutableByteArrayView {

  override def unsafeGet(index: Int): Byte = unsafeUnderlyingArray(offset + index)
}

object OffsetImmutableByteArrayView {
  def unsafeSliceFromDirectView(directView: DirectImmutableByteArrayView, offset: Int, length: Int): OffsetImmutableByteArrayView = {
    OffsetImmutableByteArrayView(directView.unsafeUnderlyingArray, offset, length)
  }

  def unsafeSliceOffsetView(offsetView: OffsetImmutableByteArrayView, offset: Int, length: Int): OffsetImmutableByteArrayView = {
    OffsetImmutableByteArrayView(offsetView.unsafeUnderlyingArray, offsetView.offset + offset, length)
  }

  def unsafeSliceExistingView(existingView: ImmutableByteArrayView, offset: Int, length: Int): OffsetImmutableByteArrayView = {
    existingView match {
      case offsetView: OffsetImmutableByteArrayView => unsafeSliceOffsetView(offsetView, offset, length)
      case directView: DirectImmutableByteArrayView => unsafeSliceFromDirectView(directView, offset, length)
    }
  }
}

final class DirectImmutableByteArrayView(override val unsafeUnderlyingArray: Array[Byte]) extends ImmutableByteArrayView {
  override def offset: Int = 0

  override def length: Int = unsafeUnderlyingArray.length

  def unsafeToArray: Array[Byte] = unsafeUnderlyingArray

  override def unsafeGet(index: Int): Byte = unsafeUnderlyingArray(index)
}

object DirectImmutableByteArrayView {
  def unsafeFromArray(array: Array[Byte]): DirectImmutableByteArrayView =
    new DirectImmutableByteArrayView(array)

  def fromArraySeq(arraySeq: ArraySeq[Byte]): DirectImmutableByteArrayView =
    new DirectImmutableByteArrayView(ArraySeqUtils.unsafeToByteArray(arraySeq))

  def apply(xs: Byte*): DirectImmutableByteArrayView = {
    val result = new Array[Byte](xs.size)
    xs.zipWithIndex.foreach{case (byte, idx) => result(idx) = byte}
    DirectImmutableByteArrayView.unsafeFromArray(result)
  }
}

final case class CheckBlock(
  parentBlocks: NonEmptySet[Int],
  payload: ImmutableByteArrayView
) {
  def toPreprocessedBlock: AuxiliaryBlock = AuxiliaryBlock(payload, parentBlocks)
}

sealed trait PreprocessedBlock {
  def underlyingArray: ImmutableByteArrayView
}

object PreprocessedBlock {
  def fromRawDecodedBlock(rawDecodedBlock: RawDecodedBlock): Option[PreprocessedBlock] = {
    OriginalMessageBlock.fromRawDecodedBlock(rawDecodedBlock)
      .orElse(AuxiliaryBlock.fromRawByteView(rawDecodedBlock.underlyingArray))
  }
}

sealed trait OriginalMessageBlock extends PreprocessedBlock {
  def originalBytes: ImmutableByteArrayView

  def blockIndex: Int
}

object OriginalMessageBlock {
  def decodeFromRawByteView(rawByteView: ImmutableByteArrayView): Option[OriginalMessageBlock] = {
    NonterminalMessageBlock.decodeFromRawByteView(rawByteView)
      .orElse(TerminalMessageBlock.decodeFromRawByteView(rawByteView))
  }

  def fromRawDecodedBlock(rawDecodedBlock: RawDecodedBlock): Option[OriginalMessageBlock] = {
    NonterminalMessageBlock.decodeFromRawByteView(rawDecodedBlock.underlyingArray)
      .orElse(TerminalMessageBlock.decodeFromRawByteView(rawDecodedBlock.underlyingArray))
  }
}

final case class RawDecodedBlock(underlyingArray: ImmutableByteArrayView)

final class NonterminalMessageBlock(override val underlyingArray: ImmutableByteArrayView) extends OriginalMessageBlock {
  override def originalBytes: ImmutableByteArrayView =
    OffsetImmutableByteArrayView.unsafeSliceExistingView(underlyingArray, 1, underlyingArray.length - 5)

  override def blockIndex: Int = underlyingArray.get(1).get
}

object NonterminalMessageBlock {
  val NonterminalMessageBlockByteMarker: Byte = 1

  def apply(payload: ImmutableByteArrayView, blockIndex: Int): NonterminalMessageBlock = {
    val resultingArray = new Array[Byte](payload.length + 5)
    payload.unsafeCopyIntoArray(5, resultingArray)
    resultingArray(0) = NonterminalMessageBlockByteMarker
    resultingArray(1) = blockIndex.asInstanceOf[Byte]
    resultingArray(2) = (blockIndex >>> 8).asInstanceOf[Byte]
    resultingArray(3) = (blockIndex >>> 16).asInstanceOf[Byte]
    resultingArray(4) = (blockIndex >>> 24).asInstanceOf[Byte]
    new NonterminalMessageBlock(new DirectImmutableByteArrayView(resultingArray))
  }

  def decodeFromRawByteView(byteView: ImmutableByteArrayView): Option[NonterminalMessageBlock] = {
    for {
      firstByte <- byteView.get(0)
      _ <- Utils.guard(firstByte == NonterminalMessageBlockByteMarker)
      // We throw away the result because we can just rewrap the view directly
      _ <- Utils.getLittleEndianIntegerAtIndex(1, byteView)
    } yield new NonterminalMessageBlock(byteView)
  }
}

final class TerminalMessageBlock(override val underlyingArray: ImmutableByteArrayView) extends OriginalMessageBlock {
  override def originalBytes: ImmutableByteArrayView =
    OffsetImmutableByteArrayView.unsafeSliceExistingView(underlyingArray, 1 + 4 + 4, underlyingArray.length - (1 + 4 + 4 + getAmountOfPadding))

  // .get justified because we assume class is valid
  def getAmountOfPadding: Int = AuxiliaryBlock.getLittleEndianIntegerAtIndex(1, underlyingArray).get

  // .get justified because we assume class is valid
  override def blockIndex: Int = underlyingArray.get(5).get
}

object TerminalMessageBlock {
  val TerminalMessageBlockByteMarker: Byte = 2

  def apply(payload: ImmutableByteArrayView, numberOfBytesPadded: Int, blockIndex: Int): TerminalMessageBlock = {
    val resultingArray = new Array[Byte](payload.length + 1 + 4 + 4 + 4 * numberOfBytesPadded)
    payload.unsafeCopyIntoArray(1, resultingArray)
    resultingArray(0) = TerminalMessageBlockByteMarker
    // Little-endian
    resultingArray(1) = numberOfBytesPadded.asInstanceOf[Byte]
    resultingArray(2) = (numberOfBytesPadded >>> 8).asInstanceOf[Byte]
    resultingArray(3) = (numberOfBytesPadded >>> 16).asInstanceOf[Byte]
    resultingArray(4) = (numberOfBytesPadded >>> 24).asInstanceOf[Byte]
    // Little-endian
    resultingArray(5) = blockIndex.asInstanceOf[Byte]
    resultingArray(6) = (blockIndex >>> 8).asInstanceOf[Byte]
    resultingArray(7) = (blockIndex >>> 16).asInstanceOf[Byte]
    resultingArray(8) = (blockIndex >>> 24).asInstanceOf[Byte]
    new TerminalMessageBlock(new DirectImmutableByteArrayView(resultingArray))
  }

  def decodeFromRawByteView(byteView: ImmutableByteArrayView): Option[TerminalMessageBlock] = {
    for {
      firstByte <- byteView.get(0)
      _ <- Utils.guard(firstByte == TerminalMessageBlockByteMarker)
      numberOfPaddedBytes <- Utils.getLittleEndianIntegerAtIndex(1, byteView)
      _ <- Utils.guard(byteView.length > numberOfPaddedBytes + 1 + 4 + 4)
    } yield new TerminalMessageBlock(byteView)
  }

}

object Utils {
  def guard(condition: Boolean): Option[Unit] = if (condition) Some(()) else None

  /**
   * byte0 is least-significant and byte3 is most-significant
   */
  private def intFromBytes(byte0: Byte, byte1: Byte, byte2: Byte, byte3: Byte): Int = {
    (byte3 << 24) + (byte2 << 16) + (byte1 << 8) + (byte0 << 0)
  }

  def getLittleEndianIntegerAtIndex(index: Int, byteArrayView: ImmutableByteArrayView): Option[Int] = {
    for {
      firstByte <- byteArrayView.get(index)
      secondByte <- byteArrayView.get(index + 1)
      thirdByte <- byteArrayView.get(index + 2)
      fourthByte <- byteArrayView.get(index + 3)
    } yield intFromBytes(firstByte, secondByte, thirdByte, fourthByte)
  }

  def xorIntoArray(arrayOfBytes: Array[Byte], immutableByteArrayView: ImmutableByteArrayView): Unit = {
    assert(immutableByteArrayView.length >= arrayOfBytes.length)
    var i = 0
    while (i < arrayOfBytes.length) {
      arrayOfBytes(i) = (arrayOfBytes(i) ^ immutableByteArrayView.unsafeGet(i)).asInstanceOf[Byte]
      i += 1
    }
  }
}

final class AuxiliaryBlock(
  override val underlyingArray: ImmutableByteArrayView
) extends PreprocessedBlock {

  def parentBlocks: NonEmptySet[Int] =
    // Unsafe is okay because we assume we've already validated this is a valid AuxiliaryBlock
    NonEmptySet.fromSetUnsafe(SortedSet.from(AuxiliaryBlock.getAllParents(underlyingArray)))

  def checkPayload: ImmutableByteArrayView = {
    // This .get is safe because we assume we've already validated this is a valid AuxiliaryBlock
    val indexOfEndingParent = AuxiliaryBlock.getIndexOfEndingParent(underlyingArray).get
    OffsetImmutableByteArrayView.unsafeSliceExistingView(underlyingArray, indexOfEndingParent, underlyingArray.length - indexOfEndingParent)
  }

  def unsafeDecode(messageBlocks: List[OriginalMessageBlock]): OriginalMessageBlock = {
    assert(messageBlocks.size == parentBlocks.length - 1)
    ???
  }
}

final case class AuxiliaryBlockDecodeIntermediateState(remainingParentBlocks: Set[BlockIndex], currentlyDecodedState: ImmutableByteArrayView)

object AuxiliaryBlock {
  val AuxiliaryBlockByteMarker: Byte = 3

  /**
   * byte0 is least-significant and byte3 is most-significant
   */
  private def intFromBytes(byte0: Byte, byte1: Byte, byte2: Byte, byte3: Byte): Int = {
    (byte3 << 24) + (byte2 << 16) + (byte1 << 8) + (byte0 << 0)
  }

  def getNumberOfExpectedParents(byteArrayView: ImmutableByteArrayView): Option[Int] = {
    getLittleEndianIntegerAtIndex(1, byteArrayView)
  }

  def getIndexOfEndingParent(byteArrayView: ImmutableByteArrayView): Option[Int] = {
    getNumberOfExpectedParents(byteArrayView).map(numberOfExpectedParents => numberOfExpectedParents * 4 + 2)
  }

  def getLittleEndianIntegerAtIndex(index: Int, byteArrayView: ImmutableByteArrayView): Option[Int] = {
    for {
      firstByte <- byteArrayView.get(index)
      secondByte <- byteArrayView.get(index + 1)
      thirdByte <- byteArrayView.get(index + 2)
      fourthByte <- byteArrayView.get(index + 3)
    } yield intFromBytes(firstByte, secondByte, thirdByte, fourthByte)
  }

  def getAllParents(byteArrayView: ImmutableByteArrayView): Set[Int] = {
    (for {
      numberOfExpectedParents <- getNumberOfExpectedParents(byteArrayView)
    } yield {
      (1 to numberOfExpectedParents)
        .toList
        .map(ithParent => getLittleEndianIntegerAtIndex((ithParent - 1) * 4 + 2, byteArrayView))
        .collect{case Some(x) => x}
        .toSet
    }).getOrElse(Set.empty)
  }

  def apply(payload: ImmutableByteArrayView, parents: NonEmptySet[Int]): AuxiliaryBlock = {
    val resultingArray = new Array[Byte](payload.length + 5 + parents.size * 4)
    payload.unsafeCopyIntoArray(1, resultingArray)
    resultingArray(0) = AuxiliaryBlockByteMarker
    val numberOfParents = parents.size
    // Little-endian
    resultingArray(1) = numberOfParents.asInstanceOf[Byte]
    resultingArray(2) = (numberOfParents >>> 8).asInstanceOf[Byte]
    resultingArray(3) = (numberOfParents >>> 16).asInstanceOf[Byte]
    resultingArray(4) = (numberOfParents >>> 24).asInstanceOf[Byte]
    parents.foreach{ parentIdx =>
      resultingArray(parentIdx * 4 + 5 + 0) = parentIdx.asInstanceOf[Byte]
      resultingArray(parentIdx * 4 + 5 + 1) = (parentIdx >>> 8).asInstanceOf[Byte]
      resultingArray(parentIdx * 4 + 5 + 2) = (parentIdx >>> 16).asInstanceOf[Byte]
      resultingArray(parentIdx * 4 + 5 + 2) = (parentIdx >>> 24).asInstanceOf[Byte]
    }
    val view = new DirectImmutableByteArrayView(resultingArray)
    new AuxiliaryBlock(view)
  }

  private def guard(condition: Boolean): Option[Unit] = if (condition) Some(()) else None

  def fromRawByteView(byteView: ImmutableByteArrayView): Option[AuxiliaryBlock] = {
    for {
      firstByte <- byteView.get(0)
      _ <- guard(firstByte == AuxiliaryBlockByteMarker)
      allParents <- NonEmptySet.fromSet(getAllParents(byteView))
    } yield AuxiliaryBlock(byteView, allParents)
  }
}

final case class NumberOfBytesPadded(toInt: Int) extends AnyVal

final case class BlockIndex(toInt: Int) extends AnyVal

object BlockIndex {
  implicit val blockIndexIsOrdered: Ordering[BlockIndex] =
    (x: BlockIndex, y: BlockIndex) => Ordering[Int].compare(x.toInt, y.toInt)
}

object OnlineCodes {

  final case class StripPreprocessDataState(
    alreadyStrippedBlocks: SortedMap[BlockIndex, OriginalMessageBlock],
    auxiliaryBlocksNotYetDecoded: Map[AuxiliaryBlock, Set[BlockIndex]],
    auxiliaryBlocksNotYetDecodedReversed: Map[BlockIndex, Set[AuxiliaryBlock]],
    numberOfOriginalBlocks: Int
  ) {

    def extractFullyDecodedMessageBlocks: Either[Set[BlockIndex], List[OriginalMessageBlock]] = {
      if (alreadyStrippedBlocks.size == numberOfOriginalBlocks) {
        Right(alreadyStrippedBlocks.values.toList)
      } else {
        Left(alreadyStrippedBlocks.keySet.diff(auxiliaryBlocksNotYetDecodedReversed.keySet))
      }
    }

    private def unsafeFullyDecodeAuxiliaryBlock(auxiliaryBlock: AuxiliaryBlock, originalMessageBlocks: List[OriginalMessageBlock]): OriginalMessageBlock = {
      val result = auxiliaryBlock.checkPayload.copyToArray()
      originalMessageBlocks.foreach(originalMessageBlock => Utils.xorIntoArray(result, originalMessageBlock.underlyingArray))
      OriginalMessageBlock.decodeFromRawByteView(DirectImmutableByteArrayView.unsafeFromArray(result)).get
    }

    def addOriginalMessageBlock(originalMessageBlock: OriginalMessageBlock): StripPreprocessDataState = {
      val affectedAuxiliaryBlocks = auxiliaryBlocksNotYetDecodedReversed
        .get(BlockIndex(originalMessageBlock.blockIndex))
        .toList
        .flatMap(_.toList)
        .map(auxBlock => auxBlock -> auxiliaryBlocksNotYetDecoded.get(auxBlock))
        .collect{case (key, Some(value)) => key -> value}
      val messageBlocksDecodedFromAuxiliaryBlocks =
        affectedAuxiliaryBlocks
          .map{
            case (affectedAuxiliaryBlock, remainingIndices) => if (remainingIndices.size == 2) {
              val alreadyExistingOriginalMessageBlocks = affectedAuxiliaryBlock
                .parentBlocks
                .toSet
                .toList
                .map(BlockIndex.apply)
                .map(alreadyStrippedBlocks.get)
                .collect{case Some(x) => x}
              val associatedOriginalMessageBlocks = originalMessageBlock :: alreadyExistingOriginalMessageBlocks
              Some(unsafeFullyDecodeAuxiliaryBlock(affectedAuxiliaryBlock, associatedOriginalMessageBlocks))
            } else {
              None
            }
          }
          .collect{case Some(x) => x}
      val newAlreadyStrippedBlocks = alreadyStrippedBlocks
        .+(BlockIndex(originalMessageBlock.blockIndex) -> originalMessageBlock)
        .++(messageBlocksDecodedFromAuxiliaryBlocks.map(block => BlockIndex(block.blockIndex) -> block))
      val newAuxiliaryBlocksNotYetDecoded = affectedAuxiliaryBlocks
        .map(_._1)
        .foldLeft(auxiliaryBlocksNotYetDecoded){
          case (acc, x) => acc.get(x) match {
            case None => acc
            case Some(y) =>
              val newSet = y.excl(BlockIndex(originalMessageBlock.blockIndex))
              if (newSet.size == 1) {
                auxiliaryBlocksNotYetDecoded.removed(x)
              } else {
                auxiliaryBlocksNotYetDecoded.updated(x, newSet)
              }
          }
        }
      val newAuxiliaryBlocksNotYetDecodedReversed = auxiliaryBlocksNotYetDecodedReversed.removed(BlockIndex(originalMessageBlock.blockIndex))
      this.copy(
        alreadyStrippedBlocks = newAlreadyStrippedBlocks,
        auxiliaryBlocksNotYetDecodedReversed = newAuxiliaryBlocksNotYetDecodedReversed,
        auxiliaryBlocksNotYetDecoded = newAuxiliaryBlocksNotYetDecoded
      )
    }

    def addAuxiliaryBlock(auxiliaryBlock: AuxiliaryBlock): StripPreprocessDataState = {
      val remainingMessageBlocks = auxiliaryBlock.parentBlocks.toSet.map(BlockIndex.apply).diff(alreadyStrippedBlocks.keySet)
      if (remainingMessageBlocks.isEmpty) {
        // This new auxiliary block doesn't help us at all since we've already decoded everything it could provide
        this
      } else if (remainingMessageBlocks.size == 1) {
        val messageBlocksToXorAgainst = auxiliaryBlock
          .parentBlocks
          .toSet
          .toList
          .map(rawIdx => alreadyStrippedBlocks.get(BlockIndex(rawIdx)))
          .collect{ case Some(x) => x }
        val result = auxiliaryBlock.checkPayload.copyToArray()
        messageBlocksToXorAgainst.foreach(originalMessageBlock => Utils.xorIntoArray(result, originalMessageBlock.underlyingArray))
        // FIXME
        val newMessageBlock = OriginalMessageBlock.decodeFromRawByteView(DirectImmutableByteArrayView.unsafeFromArray(result)).get
        this.copy(alreadyStrippedBlocks = alreadyStrippedBlocks + (BlockIndex(newMessageBlock.blockIndex) -> newMessageBlock))
      } else {
        this.copy(
          auxiliaryBlocksNotYetDecoded = auxiliaryBlocksNotYetDecoded + (auxiliaryBlock -> remainingMessageBlocks),
          auxiliaryBlocksNotYetDecodedReversed = remainingMessageBlocks
            .toList
            .foldLeft(auxiliaryBlocksNotYetDecodedReversed){
              case (acc, x) => acc.updatedWith(x){
                case None => Some(Set(auxiliaryBlock))
                case Some(exisingSet) => Some(exisingSet + auxiliaryBlock)
              }
            }
        )
      }
    }
  }

  object StripPreprocessDataState {
    def empty(numberOfOriginalBlocks: Int): StripPreprocessDataState = StripPreprocessDataState(SortedMap.empty, Map.empty, Map.empty, numberOfOriginalBlocks)
  }

  def stripPreprocessedData(preprocessedBlocks: Vector[PreprocessedBlock], numberOfOriginalBlocks: Int): Vector[ImmutableByteArrayView] = {
    preprocessedBlocks
      .foldLeft(StripPreprocessDataState.empty(numberOfOriginalBlocks)){
        case (acc, originalMessageBlock: OriginalMessageBlock) => acc.addOriginalMessageBlock(originalMessageBlock)
        case (acc, auxiliaryBlock: AuxiliaryBlock) => acc.addAuxiliaryBlock(auxiliaryBlock)
      }
      .alreadyStrippedBlocks
      .toVector
      .map(_._2.originalBytes)
  }
//
//  def preprocessData(
//    bytes: ArraySeq[Byte],
//    blockSize: Int,
//    k: Int,
//    delta0: Double,
//    randomGenerator: RandomGenerator
//  ): Vector[PreprocessedBlock] = {
//    assert(blockSize > 0)
//    val numberOfBytesPadded = bytes.size % blockSize
//    val isPadded = numberOfBytesPadded == 0
//    val numberOfBlocks = if (!isPadded) {
//      bytes.size / blockSize
//    } else {
//      bytes.size / blockSize + 1
//    }
//
//    val numberOfAuxiliaryBlocks = k * delta0 * numberOfBlocks
//
//    val originalBlocks = bytes
//      .grouped(blockSize)
//      .zipWithIndex
//      .map{case (bytes, idx) =>
//        if (idx == numberOfBlocks - 1) {
//          val numberOfBytesPadded = blockSize - bytes.size
//          TerminalMessageBlock(DirectImmutableByteArrayView.fromArraySeq(bytes), numberOfBytesPadded, idx)
//        } else {
//          NonterminalMessageBlock(DirectImmutableByteArrayView.fromArraySeq(bytes), idx)
//        }
//      }
//      .toVector
//
//    val auxiliaryBlocks = NonEmptyVector
//      .fromVector(originalBlocks)
//      .map{nonEmptyBlocks =>
//        (0 to numberOfAuxiliaryBlocks.toInt)
//          .map(_ => createSingleAuxiliaryBlockMutatesInput(nonEmptyBlocks, k, randomGenerator))
//      }
//      .getOrElse(Vector.empty)
//
//    originalBlocks ++ auxiliaryBlocks
//  }
//
//  def generateCheckBlocks(preprocessedBlocks: Vector[PreprocessedBlock], epsilon: Double, delta: Double, randomGenerator: RandomGenerator): Iterator[CheckBlock] = {
//    val arrayOfAllRhos = allRhos(epsilon, delta)
//    assert(arrayOfAllRhos.length > 0)
//    val allDegrees = (1 to (arrayOfAllRhos.length + 1)).toArray
//    val distribution = new EnumeratedIntegerDistribution(randomGenerator, allDegrees, arrayOfAllRhos)
//    new Iterator[ArraySeq[Byte]] {
//      override def hasNext: Boolean = true
//
//      override def next(): ArraySeq[Byte] =
//        createCheckBlockPrecalculated(
//          bytes,
//          ArraySeq.unsafeWrapArray(arrayOfAllRhos),
//          distribution,
//          blockSize,
//          randomGenerator
//        )
//    }
//  }
//
//  def stripPreprocessing(
//    preprocessedBytes: Vector[PreprocessedBlock],
//    blockSize: Int,
//    k: Int
//  ): ArraySeq[Byte] = {
//    ???
//  }
//
//  def createCheckBlockIterator(
//    bytes: ArraySeq[Byte],
//    blockSize: Int,
//    epsilon: Double,
//    delta: Double,
//    randomGenerator: RandomGenerator
//  ): Iterator[ArraySeq[Byte]] = {
//    val arrayOfAllRhos = allRhos(epsilon, delta)
//    assert(arrayOfAllRhos.length > 0)
//    val allDegrees = (1 to (arrayOfAllRhos.length + 1)).toArray
//    val distribution = new EnumeratedIntegerDistribution(randomGenerator, allDegrees, arrayOfAllRhos)
//    new Iterator[ArraySeq[Byte]] {
//      override def hasNext: Boolean = true
//
//      override def next(): ArraySeq[Byte] =
//        createCheckBlockPrecalculated(
//          bytes,
//          ArraySeq.unsafeWrapArray(arrayOfAllRhos),
//          distribution,
//          blockSize,
//          randomGenerator
//        )
//    }
//  }
//
//  def chooseCheckBlockFromBytesMutable0(
//    preprocessedBlocks: Vector[PreprocessedBlock],
//    degree: Int,
//    blockSize: Int,
//    randomGenerator: RandomGenerator
//  ): CheckBlock = {
//    assert(degree > 1)
//    var i = 0
//    val checkBlock = new Array[Byte](blockSize)
//    var listOfParents = SortedSet.empty[Int]
//    while (i < degree) {
//      val blockIndex = randomGenerator.nextInt(preprocessedBlocks.length)
//      val blockToXor = preprocessedBlocks(blockIndex).payload
//      xorArraysMutable(checkBlock, ArraySeqUtils.unsafeToByteArray(blockToXor))
//      i += 1
//      listOfParents = listOfParents + blockIndex
//    }
//    CheckBlock(NonEmptySet.fromSet(listOfParents), ArraySeq.unsafeWrapArray(checkBlock))
//  }
//
//  def createSingleAuxiliaryBlockMutatesInput(originalBlocks: NonEmptyVector[OriginalMessageBlock], degree: Int, randomGenerator: RandomGenerator): AuxiliaryBlock = {
//    assert(degree > 1)
//    val result = new Array[Byte](originalBlocks.head.underlyingArray.length)
//    var i = 0
//    var listOfParents = SortedSet.empty[Int]
//    while (i < degree) {
//      val blockIndex = randomGenerator.nextInt(originalBlocks.length)
//      Utils.xorIntoArray(result, originalBlocks.getUnsafe(blockIndex).underlyingArray)
//      i += 1
//      listOfParents = listOfParents + blockIndex
//    }
//    val payload = new DirectImmutableByteArrayView(result)
//    AuxiliaryBlock(payload, NonEmptyList.fromListUnsafe(listOfParents.toList))
//  }
//
//  def chooseCheckBlockFromBytesMutable(
//    bytes: ArraySeq[Byte],
//    degree: Int,
//    blockSize: Int,
//    totalNumberOfBlocks: Int,
//    randomGenerator: RandomGenerator
//  ): CheckBlock = {
//    assert(degree > 1)
//    var i = 0
//    val checkBlock = new Array[Byte](blockSize)
//    var listOfParents = SortedSet.empty[Int]
//    while (i < degree) {
//      val blockIndex = randomGenerator.nextInt(totalNumberOfBlocks)
//      val blockToXor = bytes.slice(blockIndex * blockSize, (blockIndex + 1) * blockSize)
//      xorArraysMutable(checkBlock, ArraySeqUtils.unsafeToByteArray(blockToXor))
//      i += 1
//      listOfParents = listOfParents + blockIndex
//    }
//    CheckBlock(NonEmptySet.fromSet(listOfParents), ArraySeq.unsafeWrapArray(checkBlock))
//  }
//
//  def createCheckBlockPrecalculated0(
//    preprocessedBlocks: Vector[PreprocessedBlock],
//    arrayOfAllRhos: ArraySeq[Double],
//    degreeDistribution: EnumeratedIntegerDistribution,
//    blockSize: Int,
//    randomGenerator: RandomGenerator
//  ): CheckBlock = {
//    assert(arrayOfAllRhos.nonEmpty)
//    val degree = degreeDistribution.sample()
//    chooseCheckBlockFromBytesMutable0(
//      preprocessedBlocks = preprocessedBlocks,
//      degree = degree,
//      blockSize = blockSize,
//      randomGenerator = randomGenerator
//    )
//  }
//
//  def createCheckBlockPrecalculated(
//    bytes: ArraySeq[Byte],
//    arrayOfAllRhos: ArraySeq[Double],
//    degreeDistribution: EnumeratedIntegerDistribution,
//    blockSize: Int,
//    randomGenerator: RandomGenerator
//  ): CheckBlock = {
//    assert(arrayOfAllRhos.nonEmpty)
//    val degree = degreeDistribution.sample()
//    val totalNumberOfBlocks = if (bytes.size % blockSize == 0){
//      bytes.size / blockSize
//    } else {
//      bytes.size / blockSize + 1
//    }
//    chooseCheckBlockFromBytesMutable(
//      bytes = bytes,
//      degree = degree,
//      blockSize = blockSize,
//      totalNumberOfBlocks = totalNumberOfBlocks,
//      randomGenerator = randomGenerator
//    )
//  }
//
//  def createCheckBlock(bytes: ArraySeq[Byte], epsilon: Double, delta: Double, blockSize: Int, randomGenerator: RandomGenerator): CheckBlock = {
//    val arrayOfAllRhos = allRhos(epsilon, delta)
//    assert(arrayOfAllRhos.length > 0)
//    val allDegrees = (1 to (arrayOfAllRhos.length + 1)).toArray
//    val degree = new EnumeratedIntegerDistribution(randomGenerator, allDegrees, arrayOfAllRhos).sample()
//    val totalNumberOfBlocks = if (bytes.size % blockSize == 0){
//      bytes.size / blockSize
//    } else {
//      bytes.size / blockSize + 1
//    }
//    var i = 0
//    val checkBlock = new Array[Byte](blockSize)
//    while (i < degree) {
//      val blockIndex = randomGenerator.nextInt(totalNumberOfBlocks)
//      val blockToXor = bytes.slice(blockIndex * blockSize, (blockIndex + 1) * blockSize)
//      xorArraysMutable(checkBlock, ArraySeqUtils.unsafeToByteArray(blockToXor))
//      i += 1
//    }
//    ArraySeq.unsafeWrapArray(checkBlock)
//    ???
//  }
//
//  private def unsafeXorArraysMutable0(result: Array[Byte], toXor: ImmutableByteArrayView): Unit = {
//    var i = 0
//    while (i < result.length) {
//      result(i) = (result(i) ^ toXor.unsafeGet(i)).asInstanceOf[Byte]
//      i += 1
//    }
//  }
//
//  private def xorArraysMutable(result: Array[Byte], toXor: Array[Byte]): Unit = {
//    var i = 0
//    while (i < result.length) {
//      result(i) = (result(i) ^ toXor(i)).asInstanceOf[Byte]
//      i += 1
//    }
//  }
//
//  def calculateF(epsilon: Double, delta: Double): Double = {
//    (math.log(delta) + math.log(epsilon / 2)) / math.log(1 - delta)
//  }
//
//  def calculateRho1(epsilon: Double, delta: Double): Double = {
//    1 - ((1 + 1 / calculateF(epsilon, delta)) / (1 + epsilon))
//  }
//
//  def allRhos(epsilon: Double, delta: Double): Array[Double] = {
//    val f = calculateF(epsilon, delta)
//    val rho1 = calculateRho1(epsilon, delta)
//    val maximumIndex = f.toInt
//    val result = new Array[Double](maximumIndex)
//    if (maximumIndex == 0) {
//      // Do nothing
//      ()
//    } else if (maximumIndex == 1) {
//      result(0) = 1.0
//    } else {
//      result(0) = rho1
//      var i = 1
//      while (i <= maximumIndex) {
//        result(i) = calculateRhoPrecalculatedFAndRho1(i + 1, f, rho1)
//        i += 1
//      }
//    }
//    result
//  }
//
//  def calculateRhoPrecalculatedFAndRho1(idx: Int, f: Double, rho1: Double): Double = {
//    assert(idx > 1)
//    assert(idx < f)
//    (1 - 1 / rho1) / ((1 - 1 / f) * idx * (idx - 1))
//  }
//
//  def calculateRho(idx: Int, epsilon: Double, delta: Double): Double = {
//    assert(idx > 1)
//    val f = calculateF(epsilon, delta)
//    assert(idx < f)
//    val rho1 = calculateRho1(epsilon, delta)
//    (1 - 1 / rho1) / ((1 - 1 / f) * idx * (idx - 1))
//  }

}
