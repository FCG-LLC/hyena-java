package co.llective.hyena.api

import io.airlift.slice.Slice
import io.airlift.slice.Slices
import java.io.IOException
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.*
import kotlin.experimental.and

object MessageDecoder {

    fun decode(buf: ByteBuffer): Reply {
        try {
            val responseType = decodeMessageType(buf)
            return decodeMessage(responseType, buf)
        } catch (exc: RuntimeException) {
            throw DeserializationException(exc)
        }
    }

    internal fun decodeMessageType(buf: ByteBuffer): ApiRequest {
        val messageTypeId = buf.int

        if (messageTypeId <= ApiRequest.values().size) {
            return ApiRequest.values()[messageTypeId]
        } else {
            throw throw DeserializationException("Cannot deserialize response type of index $messageTypeId")
        }
    }

    private fun decodeMessage(request: ApiRequest, buf: ByteBuffer): Reply {
        return when (request) {
            ApiRequest.ListColumns -> decodeListColumn(buf)
            ApiRequest.Insert -> InsertReply(decodeEither(buf) { b -> b.int })
            ApiRequest.Scan -> ScanReply(decodeEither(buf, this::decodeScanResult))
            ApiRequest.RefreshCatalog -> CatalogReply(decodeRefreshCatalog(buf))
            ApiRequest.AddColumn -> AddColumnReply(decodeEither(buf) { b -> b.int })
            ApiRequest.Flush -> TODO()
            ApiRequest.DataCompaction -> TODO()
            ApiRequest.SerializeError -> SerializeError(decoderSerializeError(buf))
            ApiRequest.Other -> TODO()
        }
    }

    fun decodePeerReply(buf: ByteBuffer): PeerReply {
        val replyType = PeerReplyType.values()[buf.int]
        when (replyType) {
            PeerReplyType.KeepAlive -> return KeepAliveReply()
            PeerReplyType.Response -> {
                val replyMessageId = buf.long
                val ok = buf.int
                if (ok == 0) {
                    val option = buf.get()
                    if (option == 0.toByte()) {
                        throw IOException("No data in response payload")
                    } else {
                        val bufSize = buf.long
                        return ResponseReply(replyMessageId, buf)
                    }
                } else if (ok == 1) {
                    return ResponseReplyError(replyMessageId)
                }
            }
        }
        throw IllegalStateException("Unreachable branch of decodePeerReply")
    }

    fun decodeControlReply(buf: ByteBuffer): ControlReply {
        val controlReplyType = buf.int
        val ok = buf.int
        val connectionId = buf.long
        val socketAddress = decodeString(buf)
        println("Connection id: $connectionId, dedicated socket address: $socketAddress")
        return ControlReply(connectionId, socketAddress)
    }

    data class ControlReply(val connectionId: Long, val socketAddress: String)

    @Throws(IOException::class)
    private fun decodeScanResult(buf: ByteBuffer): ScanResult {
        val columnNo = buf.long
        val columnMap = HashMap<Long, ColumnValues>()

        // process every column
        for (x in 0 until columnNo) {
            val columnId = buf.long
            val columnType = BlockType.values()[buf.int]
            val dataPresent = buf.get()
            var column: ColumnValues
            column = if (dataPresent == 0.toByte()) {
                EmptyColumn(columnType)
            } else {
                decodeColumnValues(buf)
            }
            columnMap[columnId] = column
        }
        return ScanResult(columnMap)
    }

    internal fun decodeColumnValues(buf: ByteBuffer): ColumnValues {
        val type = BlockType.values()[buf.int]
        val recordsCount = buf.long.toInt()

        val column = when {
            type == BlockType.StringDense -> {
                val slices = createSimpleStringSlices(recordsCount, buf)
                SimpleDenseStringColumn(recordsCount, slices)
            }
            type.isDense() -> {
                val slice = createDataSlice(type, recordsCount, buf)
                DenseNumberColumn(type, slice, recordsCount)
            }
            else -> {
                val slice = createDataSlice(type, recordsCount, buf)
                val indexSlice = createIndexSlice(recordsCount, buf)
                SparseNumberColumn(type, slice, indexSlice, recordsCount)
            }
        }
        return column
    }

    private fun createSimpleStringSlices(stringsNumber: Int, buf: ByteBuffer): List<Slice> {
        val strings = ArrayList<Slice>()
        for (x in 0 until stringsNumber) {
            val stringLen = buf.long.toInt()
            val stringArray = ByteArray(stringLen)
            buf.get(stringArray)
            val slice = Slices.wrappedBuffer(stringArray, 0, stringLen)
            strings.add(slice)
        }
        return strings
    }

    private fun createDataSlice(type: BlockType, recordsCount: Int, buf: ByteBuffer): Slice {
        val bytesToAllocate = recordsCount * type.size().bytes
        val dstArray = ByteArray(bytesToAllocate)
        // fill array
        buf.get(dstArray)
        // go into slice
        return Slices.wrappedBuffer(dstArray, 0, dstArray.size)
    }

    private fun createIndexSlice(recordsCount: Int, buf: ByteBuffer): Slice {
        val vectorLen = buf.long.toInt()
        if (vectorLen != recordsCount) {
            throw DeserializationException("Sparse data inconsistent, values len doesn't match offsets len")
        }
        val bytesToAllocate = recordsCount * 4 // 32bit - index size
        val dstArray = ByteArray(bytesToAllocate)
        // fill array
        buf.get(dstArray)
        // go into slice
        return Slices.wrappedBuffer(dstArray, 0, dstArray.size)
    }

    @Throws(IOException::class)
    private fun decodeRefreshCatalog(buf: ByteBuffer): Catalog {
        val columnCount = buf.long
        val columns = ArrayList<Column>()
        for (i in 0 until columnCount) {
            columns.add(decodeColumn(buf))
        }

        val partitionCount = buf.long
        val partitions = ArrayList<PartitionInfo>()
        for (i in 0 until partitionCount) {
            partitions.add(decodePartitionInfo(buf))
        }

        return Catalog(columns, partitions)
    }

    @Throws(IOException::class)
    private fun decodePartitionInfo(buf: ByteBuffer): PartitionInfo {
        val minTs = buf.long
        val maxTes = buf.long
        val uuid = decodeUuid(buf)
        return PartitionInfo(minTs, maxTes, uuid, decodeString(buf))
    }

    private fun decodeUuid(buf: ByteBuffer): UUID {
        val hi = buf.long
        val lo = buf.long
        return UUID(hi, lo)
    }

    private fun <T> decodeEither(buf: ByteBuffer, decodeOk: (buf: ByteBuffer) -> T): Either<T, ApiError> {
        val ok = buf.int
        return if (ok == 0) {
            Left(decodeOk(buf))
        } else {
            Right(decodeApiError(buf))
        }
    }

    private fun decodeApiError(buf: ByteBuffer): ApiError {
        val errorType = ApiErrorType.values()[buf.int]
        return when (errorType.type) {
            ApiErrorType.ExtraType.Long ->
                ApiError(errorType, Optional.of(buf.long))
            ApiErrorType.ExtraType.String ->
                ApiError(errorType, Optional.of(decodeString(buf)))
            ApiErrorType.ExtraType.None ->
                ApiError(errorType, Optional.empty())
        }
    }

    private fun decodeListColumn(buf: ByteBuffer): ListColumnsReply {
        val columnCount = buf.long.toInt()
        val columns = ArrayList<Column>()
        for (i in 0 until columnCount) {
            columns.add(decodeColumn(buf))
        }

        return ListColumnsReply(columns)
    }

    @Throws(IOException::class)
    private fun decoderSerializeError(buf: ByteBuffer): String {
        return decodeString(buf)
    }

    @Throws(IOException::class)
    private fun decodeColumn(buf: ByteBuffer): Column {
        val type = buf.int
        val id = buf.long
        return Column(BlockType.values()[type], id, decodeString(buf))
    }

    @Throws(IOException::class)
    private fun decodeString(buf: ByteBuffer): String {
        val len = buf.long.toInt()

        val bytes = ByteArray(len)
        buf.get(bytes, 0, len)
        return String(bytes, HyenaApi.UTF8_CHARSET)
    }

    /**
     * Function to convert signed bytes to unsigned short.
     *
     * E.g. -2 -> 254
     */
    fun Byte.toUnsignedShort(): Short {
        return ((this.toShort()) and 0xff)
    }

    private val TWO_COMPLEMENT: BigInteger = BigInteger.ONE.shiftLeft(64)

    internal fun decodeBigInt(value: Long): BigInteger {
        var bi = BigInteger.valueOf(value)
        if (bi < BigInteger.ZERO) {
            bi += TWO_COMPLEMENT
        }
        return bi
    }
}

class DeserializationException : Exception {
    constructor(msg: String) : super(msg)
    constructor(msg: String, cause: Exception) : super(msg, cause)
    constructor(cause: Exception) : this("Error during deserialization occurred", cause)
}