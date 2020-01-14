package com.changlinli.raptorqdemo

import java.io.DataOutput
import java.{lang, util}
import java.net.{DatagramPacket, DatagramSocket, InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}

import cats.Monad
import cats.effect.{Blocker, Concurrent, ContextShift, ExitCode, IO, IOApp, Resource, Sync}
import cats.implicits._
import fs2.Chunk
import fs2.io.udp.{Packet, Socket, SocketGroup}
import net.fec.openrq.decoder.DataDecoder
import net.fec.openrq.parameters.{FECParameters, ParameterChecker}
import net.fec.openrq.{EncodingPacket, OpenRQ, SerializablePacket, SymbolType}

import scala.collection.immutable.{ArraySeq, Queue, SortedSet}
import scala.collection.immutable.ArraySeq.ofByte
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.jdk.FunctionConverters._
import scala.jdk.FutureConverters._
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable, ParSeq}
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

  def readFromFile(fileName: String): IO[ArraySeq[Byte]] = IO{
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

  def encodeAsSingleBlockStream[F[_] : Sync](data: ArraySeq[Byte], symbolSize: Int): (FECParameters, fs2.Stream[F, EncodingPacket]) = {
    val (fecParameters, iterator) = encodeAsSingleBlockIterator(data, symbolSize)
    (fecParameters, fs2.Stream.fromIterator[F](iterator))
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

  def feedSinglePacket(packet: EncodingPacket, fecParameters: FECParameters, decoder: DataDecoder): Boolean = {
    decoder.sourceBlock(packet.sourceBlockNumber()).putEncodingPacket(packet)
    decoder.isDataDecoded
  }

  def feedSinglePacketSync[F[_] : Sync](packet: EncodingPacket, fecParameters: FECParameters, decoder: DataDecoder): F[Boolean] = {
    Sync[F].delay{ feedSinglePacket(packet, fecParameters, decoder) }
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

  def makeDatagramSocket[F[_] : Sync](port: Int): Resource[F, DatagramSocket] = {
    Resource.make{
      Sync[F].delay{
        val socket = new DatagramSocket(port)
        socket
      }
    }(socket => Sync[F].delay(socket.close()))
  }

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

  def downloadFile[F[_] : Concurrent : ContextShift](fileUUID: UUID): F[Unit] = {
    val dataDecoder = OpenRQ.newDecoder(UdpCommon.defaultFECParameters, 5)
    var counter: Int = 0
    GlobalResources
      .socketResourceLocalhost[F](8011)
      .use{
        (socket: Socket[F]) =>
          val serverAddress = new InetSocketAddress(InetAddress.getByName("localhost"), 8012)
          val writeOutDummyPacket = socket
            .write(FileRequest.createFileRequest(serverAddress, new UUID(0, 0)).underlyingPacket)
            .*>(Sync[F].delay(println("Sent a dummy packet!")))
          val receivePackets = socket
            .reads()
            .concurrently(fs2.Stream.eval(writeOutDummyPacket))
            .evalTap{_ =>
              Sync[F].delay(println(s"Packet received")).*>(Sync[F].delay({counter = counter + 1}))
            }
            .map(FileResponsePacket.decode(_, dataDecoder))
            .collect{case Some(fileFragment: FileFragment) => fileFragment}
            .map(_.toEncodingPacketWithDecoder(dataDecoder))
            .evalMap(BatchRaptorQDecoder.feedSinglePacketSync[F](_, UdpCommon.defaultFECParameters, dataDecoder))
            .takeWhile(finishedDecoding => !finishedDecoding)
            .compile
            .drain
          //          writeOutDummyPacket.*>(receivePackets)
          receivePackets
            .*>(ArraySeqUtils.writeToFile[F]("output_0.wav", dataDecoder.dataArray()))
            .*>(Sync[F].delay(println(s"Counter was ${counter}")))
            .*>(socket.write(StopRequest.createStopRequest(serverAddress, new UUID(0, 0)).underlyingPacket))
      }
  }
}

sealed trait RequestCode {
  def asByte: Byte
}
case object FileRequestCode extends RequestCode {
  override def asByte: Byte = 0
}
case object StopRequestCode extends RequestCode {
  override def asByte: Byte = 1
}

sealed trait ClientRequest {
  def requestCode: RequestCode
}

object ClientRequest {
  private def decodeFromPacketCanary(clientRequest: ClientRequest): Unit = clientRequest match {
    case _: FileRequest => ()
    case _: StopRequest => ()
  }
  def decodeFromPacket(udpPacket: Packet): Option[ClientRequest] =
    FileRequest.decodeFromPacket(udpPacket).orElse(StopRequest.decodeFromPacket(udpPacket))
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
final case class FileRequest(underlyingPacket: Packet) extends ClientRequest {
  // This should be read-only!
  private def rawBytesOfPacket = underlyingPacket.bytes.toBytes.values

  def getFileUUID: UUID = {
    val byteBuffer = java.nio.ByteBuffer.wrap(rawBytesOfPacket)
    val mostSignificantBits = byteBuffer.getLong()
    val leastSignificantBits = byteBuffer.getLong()
    new UUID(mostSignificantBits, leastSignificantBits)
  }

  override def requestCode: FileRequestCode.type = FileRequestCode
}

object FileRequest {
  def createFileRequest(remote: InetSocketAddress, fileUUID: UUID): FileRequest = {
    val sizeOfArray = 1 + 16
    val byteBuffer = java.nio.ByteBuffer.wrap(Array.fill[Byte](sizeOfArray)(0))
    byteBuffer.put(FileRequestCode.asByte)
    byteBuffer.putLong(fileUUID.getMostSignificantBits)
    byteBuffer.putLong(fileUUID.getLeastSignificantBits)
    val packet = Packet(remote, Chunk.bytes(byteBuffer.array()))
    FileRequest(packet)
  }

  // FIXME: Add additional checks (UUID validity)
  def decodeFromPacket(udpPacket: Packet): Option[FileRequest] = for {
    firstByte <- udpPacket.bytes.head
    result <- if (firstByte == FileRequestCode.asByte) Some(FileRequest(udpPacket)) else None
  } yield result
}

final case class StopRequest(underlyingPacket: Packet) extends ClientRequest {
  // This should be read-only!
  private def rawBytesOfPacket = underlyingPacket.bytes.toBytes.values

  def getFileUUID: UUID = {
    val byteBuffer = java.nio.ByteBuffer.wrap(rawBytesOfPacket)
    // Ignore the first byte
    byteBuffer.get()
    val mostSignificantBits = byteBuffer.getLong()
    val leastSignificantBits = byteBuffer.getLong()
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

sealed trait ResponseStatus {
  def asByte: Byte
}
case object SuccessfulFileResponseFragmentStatus extends ResponseStatus {
  override def asByte: Byte = 0
}
case object FileUUIDNotFoundStatus extends ResponseStatus {
  override def asByte: Byte =  1
}

object ResponseStatus {
  // If you see a warning about an uncovered case here, you need to add that case to addByte
  private def fromByteCanary(responseStatus: ResponseStatus): Unit = responseStatus match {
    case SuccessfulFileResponseFragmentStatus => ()
    case FileUUIDNotFoundStatus => ()
  }
  def fromByte(byte: Byte): Option[ResponseStatus] = {
    if (byte == SuccessfulFileResponseFragmentStatus.asByte) {
      Some(SuccessfulFileResponseFragmentStatus)
    } else if (byte == FileUUIDNotFoundStatus.asByte) {
      Some(FileUUIDNotFoundStatus)
    } else {
      None
    }
  }
}

// These packets all
sealed trait FileResponsePacket

final case class FileFragment(underlyingPacket: Packet) extends FileResponsePacket {
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

final case class FileUUIDNotFound(underlyingPacket: Packet) extends FileResponsePacket
object FileUUIDNotFound {
  def encode(inetSocketAddress: InetSocketAddress, uuid: UUID): FileUUIDNotFound = {
    val lengthOfArray = 1 + 4 // One byte for the initial response and then 4 for the UUID
    val rawBytes = Array.fill[Byte](lengthOfArray)(0)
    rawBytes(0) = FileUUIDNotFoundStatus.asByte
    val byteBuffer = ByteBuffer.wrap(rawBytes, 1, 4)
    byteBuffer.putLong(uuid.getMostSignificantBits)
    byteBuffer.putLong(uuid.getLeastSignificantBits)
    FileUUIDNotFound(Packet(inetSocketAddress, Chunk.bytes(byteBuffer.array())))
  }
}

object FileResponsePacket {
  def decode(udpPacket: Packet, dataDecoder: DataDecoder): Option[FileResponsePacket] = {
    if (udpPacket.bytes.size > 1000) {
      Some(FileFragment(udpPacket))
    } else {
      ResponseStatus.fromByte(udpPacket.bytes(0)).map{
        case SuccessfulFileResponseFragmentStatus => FileFragment(udpPacket)
        case FileUUIDNotFoundStatus => FileUUIDNotFound(udpPacket)
      }
    }
  }

  def lookupStatus(fileResponsePacket: FileResponsePacket): ResponseStatus = fileResponsePacket match {
    case _: FileFragment => SuccessfulFileResponseFragmentStatus
    case _: FileUUIDNotFound => FileUUIDNotFoundStatus
  }

  def encode(fileResponsePacket: FileResponsePacket): Packet = fileResponsePacket match {
    case FileFragment(underlyingPacket) => underlyingPacket
    case FileUUIDNotFound(underlyingPacket) => underlyingPacket
  }
}

object UdpCommon {
  val uuidToFileName: Map[UUID, Path] = Map(
    new UUID(0L, 0L) -> Paths.get("Track 10.wav")
  )

  val defaultFECParameters: FECParameters = FECParameters.newParameters(36510210, 10000, 1)

  def updateAndGetMoreInfo[A, B](atomicReference: AtomicReference[A])(f: A => Option[(B, A)]): Either[A, (B, A)] = {
    val updateF = (x: A) => f(x).map{case (_, newState) => newState}.getOrElse(x)
    val preGet = atomicReference.getAndUpdate(updateF.asJavaUnaryOperator)
    f(preGet).toRight(preGet)
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
  currentRequestsBeingProcessed: Set[FileRequest],
  filesWaitingTransfer: UniqueQueue[FileRequest]
) {
  def markBeingProcessed: Option[(FileRequest, ServerState)] = {
    filesWaitingTransfer.dequeueOption.map{
      case (requestToProcess, newQueue) =>
        val newState = ServerState(currentRequestsBeingProcessed + requestToProcess, newQueue)
        (requestToProcess, newState)
    }
  }

  def markClientRequestReceived(request: ClientRequest): ServerState = request match {
    case fileRequest: FileRequest => markFileRequestReceived(fileRequest)
    case cancellationRequest: StopRequest => markCancellationRequestReceived(cancellationRequest)
  }

  def markFileRequestReceived(request: FileRequest): ServerState = {
    this.copy(filesWaitingTransfer = filesWaitingTransfer.enqueue(request))
  }

  def markCancellationRequestReceived(requestToCancel: StopRequest): ServerState = {
    val correspondingFileRequest = FileRequest.createFileRequest(
      requestToCancel.underlyingPacket.remote,
      requestToCancel.getFileUUID
    )
    this.copy(currentRequestsBeingProcessed = currentRequestsBeingProcessed - correspondingFileRequest)
  }
}

object ServerState {
  def empty: ServerState = ServerState(Set.empty, UniqueQueue.empty)
}

object UdpServer {

  val myBytes: ArraySeq[Byte] = ArraySeq.from(Range(1, 100).map(int => int.toByte))

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

  def respondToRequest[F[_] : Sync](request: FileRequest): fs2.Stream[F, FileResponsePacket] = {
    val requestAddress = request.underlyingPacket.remote
    UdpCommon.uuidToFileName.get(request.getFileUUID) match {
      case None => fs2.Stream(FileUUIDNotFound.encode(requestAddress, request.getFileUUID))
      case Some(path) =>
        for {
          // FIXME need to account for lack of path
          bytes <- fs2.Stream.eval(ArraySeqUtils.readFromPath[F](path))
          encodingPacket <- RaptorQEncoder.encodeAsSingleBlockStream(bytes, 10000)._2
        } yield FileFragment.encode(requestAddress, encodingPacket)
    }
  }

  val serverState: AtomicReference[ServerState] =
    new AtomicReference[ServerState](ServerState(Set.empty, UniqueQueue.empty[FileRequest]))

  def transferFile(request: FileRequest): Iterator[FileResponsePacket] = {
    val requestAddress = request.underlyingPacket.remote
    UdpCommon.uuidToFileName.get(request.getFileUUID) match {
      case None =>
        Iterator.single(FileUUIDNotFound.encode(requestAddress, request.getFileUUID))
      case Some(path) =>
        val bytes = ArraySeqUtils.unsafeReadFromPath(path)
        val encodingPackets = RaptorQEncoder.encodeAsSingleBlockIterator(bytes, 10000)._2
        encodingPackets.map(FileFragment.encode(requestAddress, _))
    }
  }

  def unsafeProcessResponsePacket(fileResponsePacket: FileResponsePacket, datagramSocket: DatagramSocket): Unit = {
    val packet = FileResponsePacket.encode(fileResponsePacket)
    val byteBuffer = packet.bytes.toByteBuffer
    val datagramPacket = new DatagramPacket(byteBuffer.array(), byteBuffer.arrayOffset(), packet.bytes.size)
    datagramPacket.setSocketAddress(packet.remote)
    datagramSocket.send(datagramPacket)
  }

  def unsafeProcessOneElementOfServerState(serverState: AtomicReference[ServerState], datagramSocket: DatagramSocket): Unit = {
    UdpCommon.updateAndGetMoreInfo(serverState)(_.markBeingProcessed) match {
      case Right((fileRequest, _)) =>
        val iterator = transferFile(fileRequest)
        var i = 0
        Breaks.breakable{
          iterator.foreach{packet =>
            unsafeProcessResponsePacket(packet, datagramSocket)
            if (i % 100 == 0) {
              val stillShouldProcess = serverState.get().currentRequestsBeingProcessed.contains(fileRequest)
              if (!stillShouldProcess) {
                Breaks.break()
              }
            }
            i += 1
          }
        }
      case Left(_) => ()
    }
  }

  def processOneElementOfServerState[F[_] : Sync](serverState: AtomicReference[ServerState], datagramSocket: DatagramSocket): F[Unit] = {
    Sync[F].delay(unsafeProcessOneElementOfServerState(serverState, datagramSocket))
  }


  def unsafeBlockingListenToSocketOnce(socket: DatagramSocket): ClientRequest = {
    val packetReadBuffer: Array[Byte] = Array.fill[Byte](20000)(0)
    val packet = new DatagramPacket(packetReadBuffer, packetReadBuffer.length)
    socket.receive(packet);
    println(s"Received a packet!: $packet")
    val inetSocketAddress: InetSocketAddress = packet
      .getSocketAddress
      // The Java implementation returns an InetSocketAddress it just upcasts to SocketAddress
      .asInstanceOf[InetSocketAddress]
    val chunk = Chunk.bytes(packet.getData, packet.getOffset, packet.getLength)
    val fs2Packet = Packet(inetSocketAddress, chunk)
    // FIXME
    ClientRequest.decodeFromPacket(fs2Packet).get
  }

  def unsafeDealWithRequest(serverState: AtomicReference[ServerState], clientRequest: ClientRequest): Unit = {
    serverState.getAndUpdate(_.markClientRequestReceived(clientRequest))
    println(s"Updated server state! ${serverState.get()}")
  }

  def unsafeBlockingDealWithSocketOnce(socket: DatagramSocket, serverState: AtomicReference[ServerState]): Unit = {
    val request = unsafeBlockingListenToSocketOnce(socket)
    unsafeDealWithRequest(serverState, request)
  }

  def dealWithSocketOnce[F[_] : Sync : ContextShift](blocker: Blocker, socket: DatagramSocket, serverState: AtomicReference[ServerState]): F[Unit] = {
    blocker.blockOn(Sync[F].delay(unsafeBlockingDealWithSocketOnce(socket, serverState)))
  }

  def fullResponse[F[_] : Concurrent](incomingPackets: fs2.Stream[F, Packet]): fs2.Stream[F, FileResponsePacket] = {
    for {
      fileRequestQueue <- fs2.Stream.eval(fs2.concurrent.Queue.bounded[F, FileRequest](10))
      stopQueue <- fs2.Stream.eval(fs2.concurrent.Topic(StopRequest.createStopRequest(new InetSocketAddress(InetAddress.getLocalHost, 80), new UUID(0, 0))))
      result <- fileRequestQueue
        .dequeue
        .concurrently(stopQueue.subscribers.evalTap(numOfSubscribers => Sync[F].delay(println(s"NUM OF SUBSCRIBERS: $numOfSubscribers"))))
        .map{request =>
          val stopSignal = stopQueue
            .subscribe(10)
            .map{stopRequest =>
              println(s"STOP REQUEST UUID: ${stopRequest.getFileUUID}")
              println(s"REQUEST UUID: ${request.getFileUUID}")
              val fileUUIDsAgree = stopRequest.getFileUUID == request.getFileUUID
              println(s"STOP REQUEST address: ${stopRequest.underlyingPacket.remote}")
              println(s"REQUEST address: ${request.underlyingPacket.remote}")
              val addressesAgree = stopRequest.underlyingPacket.remote == request.underlyingPacket.remote
              fileUUIDsAgree && addressesAgree
            }
            .evalTap(stopSignal => Sync[F].delay(println(s"Stop signal is $stopSignal")))
            .takeWhile(x => !x, false)
            .append(fs2.Stream.eval(Sync[F].delay(println("Stop signal is shutting down..."))).drain)
          respondToRequest[F](request)
            .interruptWhen(stopSignal)
            .append(fs2.Stream.eval(Sync[F].delay(println("Stream is shutting down..."))).drain)
        }
        .parJoin(5)
        .concurrently{
          incomingPackets
            .evalTap(packet => Sync[F].delay(println(s"SERVER RECEIVED: $packet")))
            .map(ClientRequest.decodeFromPacket)
            .collect{case Some(x) => x}
            .evalMap{
              case request: FileRequest =>
                println(s"This was a file request: $request")
                fileRequestQueue.enqueue1(request)
              case request: StopRequest =>
                println(s"This was a stop request: $request")
                stopQueue.publish1(request)
            }
        }
    } yield result
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

  def fullRun[F[_] : Concurrent: ContextShift]: F[Unit] = {
    GlobalResources.blockerResource[F]
      .flatMap(blocker => GlobalResources.makeDatagramSocket[F](8012).map((blocker, _)))
      .use{
        case (blocker, socket) =>
          val processingStateStream = fs2.Stream
            .constant[F, fs2.Stream[F, Unit]](fs2.Stream.eval(processOneElementOfServerState[F](UdpServer.serverState, socket)), 1)
            .parJoin(5)
          val listeningToSocketStream = fs2.Stream.repeatEval(dealWithSocketOnce(blocker, socket, UdpServer.serverState))
          listeningToSocketStream
            .evalMap(_ => Concurrent[F].start(processOneElementOfServerState[F](UdpServer.serverState, socket)))
            .compile
            .drain
      }
//    GlobalResources.socketResourceLocalhost[F](8012)
//      .use{
//        readSocket: Socket[F] =>
//          fullResponse(readSocket.reads())
//            .map{
//              case fileFragment: FileFragment => fileFragment.underlyingPacket
//              case fileUUIDNotFound: FileUUIDNotFound => fileUUIDNotFound.underlyingPacket
//            }
//            .through(readSocket.writes()).compile.drain
//      }
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
      UdpClient.downloadFile[IO](new UUID(0, 0))
    } else if (args(1) == "server") {
      UdpServer.fullRun[IO]
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
