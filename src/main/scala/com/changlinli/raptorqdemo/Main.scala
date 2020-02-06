package com.changlinli.raptorqdemo

import java.net.{DatagramPacket, DatagramSocket, InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent._

import cats.Applicative
import cats.effect.{Blocker, Concurrent, ContextShift, ExitCode, IO, IOApp, Resource, Sync}
import cats.implicits._
import fs2.Chunk
import fs2.io.udp.{Packet, Socket, SocketGroup}
import grizzled.slf4j.Logging
import net.fec.openrq.decoder.DataDecoder
import net.fec.openrq.parameters.{FECParameters, ParameterChecker}
import net.fec.openrq.{EncodingPacket, OpenRQ}

import scala.annotation.tailrec
import scala.collection.immutable.ArraySeq.ofByte
import scala.collection.immutable.{ArraySeq, Queue}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.jdk.FunctionConverters._
import scala.language.higherKinds
import scala.util.control.Breaks

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

  def readFromFile[F[_] : Sync](fileName: String): F[ArraySeq[Byte]] = Sync[F].delay{
    ArraySeq.unsafeWrapArray(Files.readAllBytes(Paths.get(fileName)))
  }

  def unsafeReadFromPath(path: Path): ArraySeq[Byte] = {
    ArraySeq.unsafeWrapArray(Files.readAllBytes(path))
  }

  def readFromPath[F[_] : Sync](path: Path): F[ArraySeq[Byte]] = Sync[F].delay{
    unsafeReadFromPath(path)
  }

  def writeToFile[F[_] : Sync](fileName: String, bytes: Array[Byte]): F[Unit] = Sync[F].delay {
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

  def encodeAsSingleBlockIterator(data: ArraySeq[Byte], symbolSize: Int): (FECParameters, Iterator[EncodingPacket]) = {
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

  def feedSinglePacketDebug(packet: EncodingPacket, fecParameters: FECParameters, decoder: DataDecoder, idx: Int): Boolean = {
    if (idx == 3654) {
      val result = feedSinglePacket(packet, fecParameters, decoder)
      result
    } else {
      val result = feedSinglePacket(packet, fecParameters, decoder)
      result
    }
  }

  def feedSinglePacket(packet: EncodingPacket, fecParameters: FECParameters, decoder: DataDecoder): Boolean = {
    decoder.sourceBlock(packet.sourceBlockNumber()).putEncodingPacket(packet)
    decoder.isDataDecoded
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

  def makeDatagramSocket[F[_] : Sync](port: Int): Resource[F, DatagramSocket] = {
    Resource.make{
      Sync[F].delay{
        try {
          val socket = new DatagramSocket(port)
          socket
        } catch {
          case e: Exception =>
            throw new RuntimeException(s"Unable to open a UDP socket on port $port", e)
        }
      }
    }(socket => Sync[F].delay(socket.close()))
  }

}

object UdpClient extends Logging {
  private val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))

  val (fecParameters1, udpPackets) = UdpProcessing.sendAsUdp(myBytes)

  private val readBuffer = Array.fill[Byte](65535)(0)

  // WARNING: Cannot use the datagram packet between next calls!
  def unsafeBlockingPacketIterator(socket: DatagramSocket): Iterator[DatagramPacket] = {
    val packet = new DatagramPacket(readBuffer, readBuffer.length)
    new Iterator[DatagramPacket] {
      override def hasNext: Boolean = true

      override def next(): DatagramPacket = {
        socket.receive(packet)
        packet
      }
    }
  }

  def tapIterator[A](iterator: Iterator[A])(f: A => Unit): Iterator[A] = {
    iterator.map{x =>
      f(x)
      x
    }
  }

  def unsafeUploadBytes(socket: DatagramSocket, bytes: ArraySeq[Byte], addressToSendTo: InetSocketAddress, fileUUID: UUID, fileName: BoundedString): Unit = {
    val encodingPackets = RaptorQEncoder.encodeAsSingleBlockIterator(bytes, UdpCommon.symbolSize)._2
    val fileUploadRequest = FileUploadRequest.create(fileUUID, fileName, addressToSendTo)
    socket.send(UdpCommon.datagramPacketFromFS2Packet(fileUploadRequest.underlyingPacket))
    encodingPackets
      .map(FileFragment.encode(addressToSendTo, _))
      .map(_.underlyingPacket.|>(UdpCommon.datagramPacketFromFS2Packet))
      .foreach(fileFragmentPacket => socket.send(fileFragmentPacket))
  }

  def unsafeDownloadFile(socket: DatagramSocket): ArraySeq[Byte] = {
    val dataDecoder = OpenRQ.newDecoder(UdpCommon.defaultFECParameters, 5)
    val serverAddress = new InetSocketAddress(InetAddress.getByName("159.65.152.145"), 8012)
    val packet = FileDownloadRequest.createFileRequest(serverAddress, UdpCommon.fileInQuestion).underlyingPacket
    val fileRequestDatagramPacket = UdpCommon.datagramPacketFromFS2Packet(packet)
    val downloadIterator = unsafeBlockingPacketIterator(socket)
      .|>(tapIterator(_)(packet => logger.debug(s"Raw datagram packet: $packet")))
      .map(UdpCommon.fs2packetFromDatagramPacket)
//      .|>(tapIterator(_)(fs2Packet => logger.debug(s"Decoded FS2 packet: $fs2Packet")))
      .map(ServerResponse.decode(_, dataDecoder))
//      .|>(tapIterator(_)(serverResponseOpt => logger.debug(s"After attempting to decode a packet as a response from the server: $serverResponseOpt")))
      .collect{case Some(fileFragment: FileFragment) => fileFragment}
//      .|>(tapIterator(_)(serverResponse => logger.debug(s"After decoding server response: $serverResponse")))
      .map(_.toEncodingPacketWithDecoder(dataDecoder))
      .|>(tapIterator(_)(encodingPacket => logger.debug(s"After converting it to a Raptor packet: $encodingPacket")))
      .zipWithIndex
      .map{case (x, idx) => BatchRaptorQDecoder.feedSinglePacketDebug(x, UdpCommon.defaultFECParameters, dataDecoder, idx)}
      .|>(tapIterator(_)(decodeResult => logger.debug(s"After feeding the packet to our decoder have we successfully decoded yet? $decodeResult")))
      .takeWhile(finishedDecoding => !finishedDecoding)
      .|>(tapIterator(_)(_ => logger.debug("Finished decoding!")))

    socket.send(fileRequestDatagramPacket)
    logger.info("Sent file request")
    var i = 0
    downloadIterator.foreach{_ =>
      logger.debug(s"Processed packet: $i")
      i += 1
    }
    val stopRequest = StopRequest.createStopRequest(serverAddress, UdpCommon.fileInQuestion).underlyingPacket
    socket.send(UdpCommon.datagramPacketFromFS2Packet(stopRequest))
    ArraySeq.unsafeWrapArray(dataDecoder.dataArray())
  }

  def downloadFileAA[F[_] : Sync : ContextShift](blocker: Blocker, socket: DatagramSocket): F[ArraySeq[Byte]] = {
    blocker.blockOn(Sync[F].delay(unsafeDownloadFile(socket)))
  }

  def uploadFile[F[_] : Sync : ContextShift](
    blocker: Blocker,
    socket: DatagramSocket,
    bytes: ArraySeq[Byte],
    addressToSendTo: InetSocketAddress,
    fileUUID: UUID, fileName: BoundedString
  ): F[Unit] = {
    blocker.blockOn(Sync[F].delay(unsafeUploadBytes(socket, bytes, addressToSendTo, fileUUID, fileName)))
  }

  def downloadFile[F[_] : Sync : ContextShift](fileUUID: UUID): F[ArraySeq[Byte]] = {
    GlobalResources.blockerResource[F]
      .flatMap(blocker => GlobalResources.makeDatagramSocket(8011).map((_, blocker)))
      .use{
        case (socket, blocker) => downloadFileAA(blocker, socket)
      }
  }

}

sealed trait RequestCode {
  def asByte: Byte
}
case object FileDownloadRequestCode extends RequestCode {
  override def asByte: Byte = 27
}
case object StopRequestCode extends RequestCode {
  override def asByte: Byte = 1
}
case object FileUploadRequestCode extends RequestCode {
  override def asByte: Byte = 28
}

sealed trait ClientRequest {
  def requestCode: RequestCode
}

object ClientRequest {
  private def decodeFromPacketCanary(clientRequest: ClientRequest): Unit = clientRequest match {
    case _: FileDownloadRequest => ()
    case _: StopRequest => ()
    case _: FileUploadRequest => ()
  }
  def decodeFromPacket(udpPacket: Packet): Option[ClientRequest] =
    FileDownloadRequest.decodeFromPacket(udpPacket)
      .orElse(StopRequest.decodeFromPacket(udpPacket))
      .orElse(FileUploadRequest.decodeFromPacket(udpPacket))
}

/**
 * File request has the following byte structure
 *
 * First 16 bytes form a UUID. This UUID uniquely identifies a file and must
 * negotiated beforehand.
 *
 * Optionally, the next 8 bytes can be used to specify a file size.
 *
 * Also optionally, the next 8 bytes after that can be used to specify a file hash.
 *
 *
 * @param underlyingPacket
 */
final case class FileDownloadRequest(underlyingPacket: Packet) extends ClientRequest {
  // This should be read-only!
  private def rawBytesOfPacket = underlyingPacket.bytes.toBytes.values

  def fileUUID: UUID = {
    val byteBuffer = java.nio.ByteBuffer.wrap(rawBytesOfPacket)
    // Ignore the first byte, which just signals what kind of packet this is
    val mostSignificantBits = byteBuffer.getLong(1)
    val leastSignificantBits = byteBuffer.getLong(1 + 8)
    new UUID(mostSignificantBits, leastSignificantBits)
  }

  def address: InetSocketAddress = underlyingPacket.remote

  override def requestCode: FileDownloadRequestCode.type = {
    assert(
      FileDownloadRequestCode.asByte == rawBytesOfPacket.head,
      s"This is a programmer bug! We created a FileDownloadRequest around a " +
        s"packet whose first byte does not signal a FileDownloadRequest (${FileDownloadRequestCode.asByte}) (it " +
        s"was instead ${rawBytesOfPacket.head})"
    )
    FileDownloadRequestCode
  }

  override def toString: String = s"FileDownloadRequest(fileUUID: $fileUUID, address: $address)"
}

object FileDownloadRequest {
  def createFileRequest(remote: InetSocketAddress, fileUUID: UUID): FileDownloadRequest = {
    val sizeOfArray = 1 + 16
    val byteBuffer = java.nio.ByteBuffer.wrap(Array.fill[Byte](sizeOfArray)(0))
    byteBuffer.put(FileDownloadRequestCode.asByte)
    byteBuffer.putLong(fileUUID.getMostSignificantBits)
    byteBuffer.putLong(fileUUID.getLeastSignificantBits)
    val packet = Packet(remote, Chunk.bytes(byteBuffer.array()))
    FileDownloadRequest(packet)
  }

  // FIXME: Add additional checks (UUID validity)
  def decodeFromPacket(udpPacket: Packet): Option[FileDownloadRequest] = for {
    firstByte <- udpPacket.bytes.head
    result <- if (firstByte == FileDownloadRequestCode.asByte) Some(FileDownloadRequest(udpPacket)) else None
  } yield result
}

sealed abstract case class BoundedString private (underlyingString: String) {
  def toBytes: ArraySeq[Byte] = ArraySeq.unsafeWrapArray(underlyingString.getBytes(StandardCharsets.UTF_8))

  def lengthAsUnsignedByte: Byte = (toBytes.length - 127).asInstanceOf[Byte]
}

object BoundedString {
  val MaxSizeInBytes: Int = 256

  def fromString(str: String): Option[BoundedString] =
    if (str.getBytes(StandardCharsets.UTF_8).length <= MaxSizeInBytes) {
      Some(new BoundedString(str) {})
    } else {
      None
    }

  def unsafeDecodeFromBytes(bytes: ArraySeq[Byte]): BoundedString = {
    decodeFromBytes(bytes)
      .getOrElse(throw new Exception(
        s"Programmer Error! In order to use this method you must be sure that " +
          s"the size of the bytes passed in (${bytes.length}) is less than $MaxSizeInBytes"
      ))
  }

  def decodeFromBytes(bytes: ArraySeq[Byte]): Option[BoundedString] =
    if (bytes.length <= MaxSizeInBytes) {
      val string = new String(ArraySeqUtils.unsafeToByteArray(bytes), StandardCharsets.UTF_8)
      Some(new BoundedString(string) {})
    } else {
      None
    }
}

final case class FileUploadRequest(underlyingPacket: Packet) extends ClientRequest {
  // This should be read-only!
  private def rawBytesOfPacket = underlyingPacket.bytes.toBytes.values

  def fileUUID: UUID = {
    val byteBuffer = java.nio.ByteBuffer.wrap(rawBytesOfPacket)
    // Ignore the first byte, which just signals what kind of packet this is
    val mostSignificantBits = byteBuffer.getLong(1)
    val leastSignificantBits = byteBuffer.getLong(1 + 8)
    new UUID(mostSignificantBits, leastSignificantBits)
  }

  def fileName: BoundedString = {
    // Ignore the first byte, which just signals what kind of packet this is
    // Also ignore the UUID bytes
    val indexOfFileNameSize = 1 + 16
    // We treat the size byte as an unsigned byte
    val sizeOfFileName = java.lang.Byte.toUnsignedInt(rawBytesOfPacket(indexOfFileNameSize))
    val indexOfFirstByteOfFileName = indexOfFileNameSize + 1
    val resultingBytes = Array.fill[Byte](sizeOfFileName)(0)
    Array.copy(rawBytesOfPacket, indexOfFirstByteOfFileName, resultingBytes, 0, sizeOfFileName)
    BoundedString.unsafeDecodeFromBytes(ArraySeq.unsafeWrapArray(resultingBytes))
  }

  override def requestCode: FileUploadRequestCode.type = {
    assert(
      FileUploadRequestCode.asByte == rawBytesOfPacket.head,
      s"This is a programmer bug! We created a FileUploadRequest around a " +
        s"packet whose first byte does not signal a FileUploadRequest (${FileUploadRequestCode.asByte}) (it " +
        s"was instead ${rawBytesOfPacket.head})"
    )
    FileUploadRequestCode
  }
}

object FileUploadRequest {
  def create(fileUUID: UUID, fileName: BoundedString, address: InetSocketAddress): FileUploadRequest = {
    val size = 16 + 1 + 1 + fileName.toBytes.length
    val rawBytes = Array.fill[Byte](size)(0)
    val byteBuffer = ByteBuffer.wrap(rawBytes)
    byteBuffer.put(FileUploadRequestCode.asByte)
    byteBuffer.putLong(fileUUID.getMostSignificantBits)
    byteBuffer.putLong(fileUUID.getLeastSignificantBits)
    byteBuffer.put(fileName.lengthAsUnsignedByte)
    byteBuffer.put(ArraySeqUtils.unsafeToByteArray(fileName.toBytes))
    val chunk = Chunk.bytes(byteBuffer.array())
    FileUploadRequest(Packet(address, chunk))
  }

  def decodeFromPacket(udpPacket: Packet): Option[FileUploadRequest] = {
    // first identifying byte + 16 bytes for a UUID + 1 byte indicating size of filename
    val minimumExpectedSize = 1 + 16 + 1
    val indexOfFileNameSizeByte = minimumExpectedSize - 1
    val sizeExceedsMinimum = udpPacket.bytes.size >= minimumExpectedSize
    val isFirstByteUploadRequestOpt =
      udpPacket.bytes.head.map(firstByte => firstByte == FileUploadRequestCode.asByte)
    val totalExpectedSizeOpt = if (sizeExceedsMinimum) {
      // Unsafe get on the Option is okay because we've checked our length exceeds the index
      // We're treating the byte as unsigned because filesize is a natural number
      val fileNameSize = java.lang.Byte.toUnsignedInt(udpPacket.bytes.get(indexOfFileNameSizeByte).get)
      Some(minimumExpectedSize + fileNameSize)
    } else {
      None
    }
    for {
      isFirstByteUploadRequest <- isFirstByteUploadRequestOpt
      totalExpectedSize <- totalExpectedSizeOpt
      _ <- if (isFirstByteUploadRequest && udpPacket.bytes.size == totalExpectedSize) Some(()) else None
    } yield FileUploadRequest(udpPacket)
  }
}

final case class StopRequest(underlyingPacket: Packet) extends ClientRequest {
  // This should be read-only!
  private def rawBytesOfPacket = underlyingPacket.bytes.toBytes.values

  def getFileUUID: UUID = {
    val byteBuffer = java.nio.ByteBuffer.wrap(rawBytesOfPacket)
    // Ignore the first byte
    byteBuffer.get()
    val mostSignificantBits = byteBuffer.getLong(1)
    val leastSignificantBits = byteBuffer.getLong(1 + 8)
    new UUID(mostSignificantBits, leastSignificantBits)
  }

  override def requestCode: StopRequestCode.type = StopRequestCode
}

object StopRequest {
  // FIXME: Add additional checks (UUID validity)
  def decodeFromPacket(udpPacket: Packet): Option[StopRequest] = for {
    firstByte <- udpPacket.bytes.head
    result <- if (firstByte == StopRequestCode.asByte) Some(StopRequest(udpPacket)) else None
  } yield result

  def createStopRequest(remote: InetSocketAddress, fileUUID: UUID): StopRequest = {
    val arraySize = 1 + 16
    val byteBuffer = java.nio.ByteBuffer.wrap(Array.fill[Byte](arraySize)(0))
    byteBuffer.put(StopRequestCode.asByte)
    byteBuffer.putLong(fileUUID.getMostSignificantBits)
    byteBuffer.putLong(fileUUID.getLeastSignificantBits)
    val packet = Packet(remote, Chunk.bytes(byteBuffer.array()))
    StopRequest(packet)
  }
}

sealed trait ResponseType {
  def asByte: Byte
}
case object SuccessfulFileResponseFragmentType extends ResponseType {
  override def asByte: Byte = 0
}
case object FileUUIDNotFoundType extends ResponseType {
  override def asByte: Byte =  1
}
case object ReceivedUploadRequestType extends ResponseType {
  override def asByte: Byte = 2
}

object ResponseType {
  // If you see a warning about an uncovered case here, you need to add that case to addByte
  private def fromByteCanary(responseStatus: ResponseType): Unit = responseStatus match {
    case SuccessfulFileResponseFragmentType => ()
    case FileUUIDNotFoundType => ()
    case ReceivedUploadRequestType => ()
  }
  def fromByte(byte: Byte): Option[ResponseType] = {
    if (byte == SuccessfulFileResponseFragmentType.asByte) {
      Some(SuccessfulFileResponseFragmentType)
    } else if (byte == FileUUIDNotFoundType.asByte) {
      Some(FileUUIDNotFoundType)
    } else if (byte == ReceivedUploadRequestType.asByte) {
      Some(ReceivedUploadRequestType)
    } else {
      None
    }
  }
}

// These packets all
sealed trait ServerResponse

final case class FileFragment(underlyingPacket: Packet) extends ServerResponse {
  def toEncodingPacketWithDecoder(dataDecoder: DataDecoder): EncodingPacket = {
    EncodingPacket.parsePacket(dataDecoder, underlyingPacket.bytes.toBytes.values, false).value()
  }
  def toEncodingPacket(fecParameters: FECParameters): EncodingPacket = {
    toEncodingPacketWithDecoder(OpenRQ.newDecoder(fecParameters, 5))
  }
}

object FileFragment {
  def encode(inetSocketAddress: InetSocketAddress, encodingPacket: EncodingPacket): FileFragment = {
    FileFragment(Packet(inetSocketAddress, Chunk.bytes(encodingPacket.asArray())))
  }
}

final case class FileUUIDNotFound(underlyingPacket: Packet) extends ServerResponse
object FileUUIDNotFound extends Logging {
  def encode(inetSocketAddress: InetSocketAddress, uuid: UUID): FileUUIDNotFound = {
    try {
      val lengthOfArray = 1 + 16 // One byte for the initial response and then 4 for the UUID
      val rawBytes = Array.fill[Byte](lengthOfArray)(0)
      rawBytes(0) = FileUUIDNotFoundType.asByte
      val byteBuffer = ByteBuffer.wrap(rawBytes, 1, 16)
      byteBuffer.putLong(uuid.getMostSignificantBits)
      byteBuffer.putLong(uuid.getLeastSignificantBits)
      FileUUIDNotFound(Packet(inetSocketAddress, Chunk.bytes(byteBuffer.array())))
    } catch {
      case exception: Exception =>
        // FIXME: Need to figure out why this error isn't actually being thrown further up
        logger.error(exception)
        throw exception
    }
  }
}

final case class ReceivedUploadRequest(underlyingPacket: Packet) extends ServerResponse
object ReceivedUploadRequest extends Logging {
  def encode(inetSocketAddress: InetSocketAddress, fileUUID: UUID): ReceivedUploadRequest = {
    try {
      val lengthOfArray = 1 + 16 // One byte for the initial response and then 4 for the UUID
      val rawBytes = Array.fill[Byte](lengthOfArray)(0)
      rawBytes(0) = ReceivedUploadRequestType.asByte
      val byteBuffer = ByteBuffer.wrap(rawBytes, 1, 16)
      byteBuffer.putLong(fileUUID.getMostSignificantBits)
      byteBuffer.putLong(fileUUID.getLeastSignificantBits)
      ReceivedUploadRequest(Packet(inetSocketAddress, Chunk.bytes(byteBuffer.array())))
    } catch {
      case exception: Exception =>
        // FIXME: Need to figure out why this error isn't actually being thrown further up
        logger.error(exception)
        throw exception
    }
  }
}

object ServerResponse {
  def decode(udpPacket: Packet, dataDecoder: DataDecoder): Option[ServerResponse] = {
    if (udpPacket.bytes.size > 1000) {
      Some(FileFragment(udpPacket))
    } else {
      ResponseType.fromByte(udpPacket.bytes(0)).map{
        case SuccessfulFileResponseFragmentType => FileFragment(udpPacket)
        case FileUUIDNotFoundType => FileUUIDNotFound(udpPacket)
        case SuccessfulFileResponseFragmentType => ???
      }
    }
  }

  def lookupStatus(fileResponsePacket: ServerResponse): ResponseType = fileResponsePacket match {
    case _: FileFragment => SuccessfulFileResponseFragmentType
    case _: FileUUIDNotFound => FileUUIDNotFoundType
  }

  def encode(fileResponsePacket: ServerResponse): Packet = fileResponsePacket match {
    case FileFragment(underlyingPacket) => underlyingPacket
    case FileUUIDNotFound(underlyingPacket) => underlyingPacket
  }
}

object UdpCommon {
  val uuidToFileName: Map[UUID, (Path, Long)] = Map(
    new UUID(0L, 0L) -> (Paths.get("Track 10.wav"), 36510210),
    new UUID(0L, 1L) -> (Paths.get("test_output_1.txt"), 210124)
  )

  val fileInQuestion = new UUID(0L, 0L)

  val symbolSize = 60000

  val defaultFECParameters: FECParameters = FECParameters.newParameters(uuidToFileName(fileInQuestion)._2, symbolSize, 1)

  def updateAndGetMoreInfo[A, B](atomicReference: AtomicReference[A])(f: A => Option[(B, A)]): Either[A, (B, A)] = {
    val updateF = (x: A) => f(x).map{case (_, newState) => newState}.getOrElse(x)
    val preGet = atomicReference.getAndUpdate(updateF.asJavaUnaryOperator)
    f(preGet).toRight(preGet)
  }

  def fs2packetFromDatagramPacket(packet: DatagramPacket): Packet = {
    val inetSocketAddress: InetSocketAddress = packet
      .getSocketAddress
      // The Java implementation returns an InetSocketAddress it just upcasts to SocketAddress
      .asInstanceOf[InetSocketAddress]
    val chunk = Chunk.bytes(packet.getData, packet.getOffset, packet.getLength)
    Packet(inetSocketAddress, chunk)
  }

  def datagramPacketFromFS2Packet(fs2Packet: Packet): DatagramPacket = {
    val byteBuffer = fs2Packet.bytes.toByteBuffer
    val datagramPacket = new DatagramPacket(byteBuffer.array(), byteBuffer.arrayOffset(), fs2Packet.bytes.size)
    datagramPacket.setSocketAddress(fs2Packet.remote)
    datagramPacket
  }
}

sealed abstract case class UniqueQueue[A](toQueue: Queue[A], private val uniquenessSet: Set[A]) {

  def dequeueOption: Option[(A, UniqueQueue[A])] = {
    toQueue.dequeueOption.map{
      case (x, newQueue) => (x, new UniqueQueue(newQueue, uniquenessSet - x) {})
    }
  }

  def enqueue(x: A): UniqueQueue[A] = {
    if (uniquenessSet.contains(x)) {
      this
    } else {
      new UniqueQueue(toQueue.enqueue(x), uniquenessSet + x) {}
    }
  }

  def size: Int = toQueue.size

  def length: Int = toQueue.length

}

object UniqueQueue {
  def empty[A]: UniqueQueue[A] = new UniqueQueue[A](Queue.empty[A], Set.empty[A]) {}
}

final case class ServerState(
  currentClientDownloadRequestsBeingProcessed: Set[FileDownloadRequest],
  currentClientUploadRequestsBeingProcessed: Set[FileUploadRequest],
  filesWaitingTransferToClients: UniqueQueue[FileDownloadRequest],
  filesWaitingTransferFromClients: UniqueQueue[FileUploadRequest],
  mappingOfUUIDsToFiles: Map[UUID, (Path, Long)]
) {
  def abbreviatedToString: String =
    s"ServerState(currentRequestsBeingProcessed: ${currentClientDownloadRequestsBeingProcessed.size} elements, filesWaitingTransfer: ${filesWaitingTransferToClients.size} elements)"

  def markTransferFromClientBeingProcessed: Option[(FileUploadRequest, ServerState)] = {
    filesWaitingTransferFromClients.dequeueOption.map{
      case (requestToProcess, newQueue) =>
        val newState = this.copy(
          currentClientUploadRequestsBeingProcessed = currentClientUploadRequestsBeingProcessed + requestToProcess,
          filesWaitingTransferFromClients = newQueue
        )
        (requestToProcess, newState)
    }
  }

  def markTransferToClientBeingProcessed: Option[(FileDownloadRequest, ServerState)] = {
    filesWaitingTransferToClients.dequeueOption.map{
      case (requestToProcess, newQueue) =>
        val newState = this.copy(
          currentClientDownloadRequestsBeingProcessed = currentClientDownloadRequestsBeingProcessed + requestToProcess,
          filesWaitingTransferToClients = newQueue
        )
        (requestToProcess, newState)
    }
  }

  def markClientRequestReceived(request: ClientRequest): ServerState = request match {
    case fileRequest: FileDownloadRequest => markFileRequestReceived(fileRequest)
    case cancellationRequest: StopRequest => markCancellationRequestReceived(cancellationRequest)
    case uploadRequest: FileUploadRequest => markFileUploadRequestReceived(uploadRequest)
  }

  def markFileRequestReceived(request: FileDownloadRequest): ServerState = {
    this.copy(filesWaitingTransferToClients = filesWaitingTransferToClients.enqueue(request))
  }

  def markCancellationRequestReceived(requestToCancel: StopRequest): ServerState = {
    val correspondingFileRequest = FileDownloadRequest.createFileRequest(
      requestToCancel.underlyingPacket.remote,
      requestToCancel.getFileUUID
    )
    this.copy(currentClientDownloadRequestsBeingProcessed = currentClientDownloadRequestsBeingProcessed - correspondingFileRequest)
  }

  def markFileUploadRequestReceived(fileUploadRequest: FileUploadRequest): ServerState = {
    this.copy(filesWaitingTransferFromClients = filesWaitingTransferFromClients.enqueue(fileUploadRequest))
  }
}

object ServerState {
  def empty: ServerState = ServerState(Set.empty, Set.empty, UniqueQueue.empty, UniqueQueue.empty, UdpCommon.uuidToFileName)
}

object UdpServer extends Logging {

  val myBytes: ArraySeq[Byte] = ArraySeq.from(Range(1, 100).map(int => int.toByte))

  val (fecParameters1, udpPackets) = UdpProcessing.sendAsUdp(myBytes)

  val serverState: AtomicReference[ServerState] = new AtomicReference[ServerState](ServerState.empty)

  def transferFile(request: FileDownloadRequest): Iterator[ServerResponse] = {
    logger.info(s"REQUEST: $request")
    val requestAddress = request.address
    val result = UdpCommon.uuidToFileName.get(request.fileUUID) match {
      case None =>
        Iterator.single(FileUUIDNotFound.encode(requestAddress, request.fileUUID))
      case Some((path, _)) =>
        val bytes = ArraySeqUtils.unsafeReadFromPath(path)
        val encodingPackets = RaptorQEncoder.encodeAsSingleBlockIterator(bytes, UdpCommon.symbolSize)._2
        encodingPackets.map(FileFragment.encode(requestAddress, _))
    }
    result
  }

  def unsafeProcessResponsePacket(fileResponsePacket: ServerResponse, datagramSocket: DatagramSocket): Unit = {
    val packet = ServerResponse.encode(fileResponsePacket)
    val datagramPacket = UdpCommon.datagramPacketFromFS2Packet(packet)
    datagramSocket.send(datagramPacket)
    logger.debug("Sent file response packet")
  }

  def unsafeProcessOneTransferFromClientInServerState(serverState: AtomicReference[ServerState], datagramSocket: DatagramSocket): Unit = {
    logger.info("Received prompt to analyze server state once: process transfer from ")
//    UdpCommon.updateAndGetMoreInfo(serverState)(_.markTransferFromClientBeingProcessed) match {
//      case Right((uploadFileRequest, _)) =>
//        logger.info("Updated state successfully")
//        val iterator = transferFile(uploadFileRequest)
//        logger.info("Created iterator!")
//        var i = 0
//        Breaks.breakable{
//          iterator.foreach{packet =>
//            unsafeProcessResponsePacket(packet, datagramSocket)
//            if (i % 100 == 0) {
//              val stillShouldProcess = serverState.get().currentClientDownloadRequestsBeingProcessed.contains(uploadFileRequest)
//              if (!stillShouldProcess) {
//                Breaks.break()
//              }
//              logger.debug(s"WE'VE processed: $i")
//            }
//            i += 1
//          }
//        }
//      case Left(x) =>
//        logger.info(s"No outstanding requests so not doing anything...: $x")
//        ()
//    }
    ???
  }

  def unsafeProcessOneElementOfServerState(serverState: AtomicReference[ServerState], datagramSocket: DatagramSocket): Unit = {
    logger.info("Received prompt to analyze server state once")
    UdpCommon.updateAndGetMoreInfo(serverState)(_.markTransferToClientBeingProcessed) match {
      case Right((fileRequest, _)) =>
        logger.info("Updated state successfully")
        val iterator = transferFile(fileRequest)
        logger.info("Created iterator!")
        var i = 0
        Breaks.breakable{
          iterator.foreach{packet =>
            unsafeProcessResponsePacket(packet, datagramSocket)
            if (i % 100 == 0) {
              val stillShouldProcess = serverState.get().currentClientDownloadRequestsBeingProcessed.contains(fileRequest)
              if (!stillShouldProcess) {
                Breaks.break()
              }
              logger.debug(s"WE'VE processed: $i")
            }
            i += 1
          }
        }
      case Left(x) =>
        logger.info(s"No outstanding requests so not doing anything...: $x")
        ()
    }
  }

  def processOneElementOfServerState[F[_] : Sync](serverState: AtomicReference[ServerState], datagramSocket: DatagramSocket): F[Unit] = {
    Sync[F].delay(unsafeProcessOneElementOfServerState(serverState, datagramSocket))
  }


  def unsafeBlockingListenToSocketOnce(socket: DatagramSocket): ClientRequest = {
    val packetReadBuffer: Array[Byte] = Array.fill[Byte](65535)(0)
    val packet = new DatagramPacket(packetReadBuffer, packetReadBuffer.length)
    socket.receive(packet)
    val fs2Packet = UdpCommon.fs2packetFromDatagramPacket(packet)
    // FIXME
    ClientRequest.decodeFromPacket(fs2Packet).getOrElse(throw new Exception(s"BLAH: $fs2Packet"))
  }

  def unsafeDealWithRequest(serverState: AtomicReference[ServerState], clientRequest: ClientRequest): Unit = {
    logger.info(s"Received this client request: $clientRequest")
    serverState.getAndUpdate(_.markClientRequestReceived(clientRequest))
    logger.info(s"Updated server state! ${serverState.get().abbreviatedToString}")
  }

  def unsafeBlockingDealWithSocketOnce(socket: DatagramSocket, serverState: AtomicReference[ServerState]): Unit = {
    val request = unsafeBlockingListenToSocketOnce(socket)
    unsafeDealWithRequest(serverState, request)
  }

  def dealWithSocketOnce[F[_] : Sync : ContextShift](blocker: Blocker, socket: DatagramSocket, serverState: AtomicReference[ServerState]): F[Unit] = {
    blocker.blockOn(Sync[F].delay(unsafeBlockingDealWithSocketOnce(socket, serverState)))
  }

  def run[F[_] : Concurrent: ContextShift]: F[Unit] = {
    GlobalResources.blockerResource[F]
      .flatMap(blocker => GlobalResources.makeDatagramSocket[F](8012).map(socket => (blocker, socket)))
      .use{
        case (blocker, socket) =>
          val listeningToSocketStream = fs2.Stream.repeatEval(dealWithSocketOnce(blocker, socket, UdpServer.serverState))
          listeningToSocketStream
            .evalMap(_ => Concurrent[F].start(processOneElementOfServerState[F](UdpServer.serverState, socket)))
            .compile
            .drain
      }
  }
}

final class DummySocket(val port: Int) extends DatagramSocket with Logging {

  private def copyDatagramPacket(packet: DatagramPacket): DatagramPacket = {
    val newBackingArray = Array.fill[Byte](packet.getLength)(0)
    Array.copy(packet.getData, packet.getOffset, newBackingArray, 0, packet.getLength)
    val newPacket = new DatagramPacket(newBackingArray, 0, newBackingArray.length)
    newPacket.setSocketAddress(packet.getSocketAddress)
    newPacket
  }

  override def send(p: DatagramPacket): Unit = {
    // FIXME deal with this cast
    p.getSocketAddress match {
      case inetSocketAddress: InetSocketAddress =>
        val packetPort = inetSocketAddress.getPort
        val packetCopy = copyDatagramPacket(p)
        // We want to mark where this packet came from
        packetCopy.setPort(port)
        DummySocket.udpChannels.compute(
          UdpPort(packetPort), (_, queue) => {
            if (queue == null) {
              Queue(packetCopy)
            } else if (packetCopy.getData.hashCode % 2 == 0) {
              queue.enqueue(packetCopy)
            } else {
              // Do nothing
              queue
            }
          }
        )
      case notInetSocketAddress =>
        logger.warn(s"Dropping this packet: $p because its socket address was not an InetSocketAddress but was instead a ${p.getSocketAddress}")
    }
  }

  @tailrec
  override def receive(p: DatagramPacket): Unit = {
    // Yay Java APIs for maximum ugliness!
    var element: Option[DatagramPacket] = None
    DummySocket.udpChannels.compute(
      UdpPort(port), (_, queue) => {
        if (queue == null) {
          Queue.empty[DatagramPacket]
        } else {
          queue.dequeueOption.map {
            case (packet, oldQueue) =>
              element = Some(packet)
              oldQueue
          }.getOrElse(Queue.empty[DatagramPacket])
        }
      }
    )
    element match {
      case Some(packet) =>
        p.setSocketAddress(packet.getSocketAddress)
        val backingArrayOfRecipient = p.getData
        p.setLength(packet.getLength)
        var i = 0
        val backingPacketArray = packet.getData
        while (i < packet.getLength) {
          val currentWriteIndex = p.getOffset + i
          backingArrayOfRecipient(currentWriteIndex) = backingPacketArray(i)
          i += 1
        }
      case None =>
        // So we don't eat CPU like a mad man
        Thread.sleep(5)
        receive(p)
    }
  }

  override def close(): Unit = {
  }

}

final case class UdpPort(toInt: Int) extends AnyVal

object DummySocket {

  val udpChannels: ConcurrentHashMap[UdpPort, Queue[DatagramPacket]] = new ConcurrentHashMap()

  val listOfPackets0: LinkedBlockingQueue[DatagramPacket] = new LinkedBlockingQueue[DatagramPacket]()
  val listOfPackets1: LinkedBlockingQueue[DatagramPacket] = new LinkedBlockingQueue[DatagramPacket]()

  def asResource[F[_] : Applicative](port: Int): Resource[F, DatagramSocket] =
    Resource.make(Applicative[F].pure[DatagramSocket](new DummySocket(port)))(_ => Applicative[F].pure(()))
}

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val myBytes = ArraySeq.from(Range(1, 100).map(int => int.toByte))
    val (fecParameters, encodedBytes) = RaptorQEncoder.encode(myBytes, 2, 5)
    val loseALotOfEncodedBytes = encodedBytes.dropWhile(packet => packet.encodingSymbolID() % 2 == 0)
    val result = BatchRaptorQDecoder.batchDecode(loseALotOfEncodedBytes.take(1000).toList, fecParameters)
    println(s"Hello world!: $result")

    val action = if (args(1) == "download") {
      UdpClient.downloadFile[IO](UdpCommon.fileInQuestion).map(_ => ())
    } else if (args(1) == "server") {
      UdpServer.run[IO]
    } else if (args(1) == "combined") {
      for {
        serverFiber <- UdpServer.run[IO].start
        _ <- IO.sleep(FiniteDuration(1, TimeUnit.SECONDS))
        clientFiber <- UdpClient.downloadFile[IO](UdpCommon.fileInQuestion).start
        _ <- serverFiber.join
        _ <- clientFiber.join
      } yield ()
    } else if (args(1) == "upload") {
      val fileNameOnDisk = args(2)
      // FIXME
      val fileName = BoundedString.fromString(fileNameOnDisk).get
      GlobalResources.blockerResource[IO]
        .flatMap(blocker => GlobalResources.makeDatagramSocket[IO](8011).map((_, blocker)))
        .use{
          case (socket, blocker) =>
            for {
              fileBytes <- ArraySeqUtils.readFromFile[IO](fileNameOnDisk)
              _ <- UdpClient.uploadFile[IO](blocker, socket, fileBytes, new InetSocketAddress(8012), new UUID(1L, 1L), fileName)
            } yield ()
        }
    } else {
      IO{
        println("This doesn't do anything")
      }
    }
    for {
      _ <- action
      _ <- IO(println("We're done with our action"))
    } yield ExitCode.Success
  }
}
