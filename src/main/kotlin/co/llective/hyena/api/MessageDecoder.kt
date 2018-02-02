package co.llective.hyena.api

import java.io.IOException
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.*

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
            throw throw DeserializationException("Cannot deserialize response type of index " + messageTypeId)
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

    @Throws(IOException::class)
    private fun decodeScanResult(buf: ByteBuffer): ScanResult {
        val data = ArrayList<DataTriple>()
        val listLength = buf.long

        for (x in 0 until listLength) {
            data.add(decodeDataTriple(buf))
        }
        return ScanResult(data)
    }

    private fun decodeDataTriple(buf: ByteBuffer): DataTriple {
        val columnId = buf.long
        val columnType = BlockType.values()[buf.int]
        val dataPresent = buf.get()
        return if (dataPresent == 0.toByte()) {
            DataTriple(columnId, columnType, Optional.empty())
        } else {
            val data = decodeBlockHolder(buf)
            DataTriple(columnId, columnType, Optional.of(data))
        }
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

    @Throws(IOException::class)
    private fun decodeBlockHolder(buf: ByteBuffer): BlockHolder {
        val type = BlockType.values()[buf.int]
        val recordsCount = buf.long.toInt()

        val block = when (type) {
            BlockType.String -> StringBlock(recordsCount)
            BlockType.I8Dense -> DenseBlock<Byte>(type, recordsCount)
            BlockType.I16Dense -> DenseBlock<Short>(type, recordsCount)
            BlockType.I32Dense -> DenseBlock<Int>(type, recordsCount)
            BlockType.I64Dense -> DenseBlock<Long>(type, recordsCount)
            BlockType.U8Dense -> DenseBlock<Short>(type, recordsCount)
            BlockType.U16Dense -> DenseBlock<Int>(type, recordsCount)
            BlockType.U32Dense -> DenseBlock<Long>(type, recordsCount)
            BlockType.U64Dense -> DenseBlock<BigInteger>(type, recordsCount)
            BlockType.I8Sparse -> SparseBlock<Byte>(type, recordsCount)
            BlockType.I16Sparse -> SparseBlock<Short>(type, recordsCount)
            BlockType.I32Sparse -> SparseBlock<Int>(type, recordsCount)
            BlockType.I64Sparse -> SparseBlock<Long>(type, recordsCount)
            BlockType.U8Sparse -> SparseBlock<Short>(type, recordsCount)
            BlockType.U16Sparse -> SparseBlock<Int>(type, recordsCount)
            BlockType.U32Sparse -> SparseBlock<Long>(type, recordsCount)
            BlockType.U64Sparse -> SparseBlock<BigInteger>(type, recordsCount)
            else -> {
                // 128 bit
                TODO("implement")
            }
        }

        for (i in 0 until recordsCount) {
            when (type) {
                BlockType.String -> (block as StringBlock).add(buf.int, buf.long)
                BlockType.I8Dense -> (block as DenseBlock<Byte>).add(buf.get())
                BlockType.I16Dense -> (block as DenseBlock<Short>).add(buf.short)
                BlockType.I32Dense -> (block as DenseBlock<Int>).add(buf.int)
                BlockType.I64Dense -> (block as DenseBlock<Long>).add(buf.long)
                BlockType.U8Dense -> (block as DenseBlock<Short>).add(buf.get().toShort())
                BlockType.U16Dense -> (block as DenseBlock<Int>).add(buf.short.toInt())
                BlockType.U32Dense -> (block as DenseBlock<Long>).add(buf.int.toLong())
                BlockType.U64Dense -> (block as DenseBlock<BigInteger>).add(decodeBigInt(buf.long))
                BlockType.I8Sparse -> (block as SparseBlock<Byte>).add(buf.int, buf.get())
                BlockType.I16Sparse -> (block as SparseBlock<Short>).add(buf.int, buf.short)
                BlockType.I32Sparse -> (block as SparseBlock<Int>).add(buf.int, buf.int)
                BlockType.I64Sparse -> (block as SparseBlock<Long>).add(buf.int, buf.long)
                BlockType.U8Sparse -> (block as SparseBlock<Short>).add(buf.int, buf.get().toShort())
                BlockType.U16Sparse -> (block as SparseBlock<Int>).add(buf.int, buf.short.toInt())
                BlockType.U32Sparse -> (block as SparseBlock<Long>).add(buf.int, buf.int.toLong())
                BlockType.U64Sparse -> (block as SparseBlock<BigInteger>).add(buf.int, decodeBigInt(buf.long))
                BlockType.I128Dense -> TODO()
                BlockType.U128Dense -> TODO()
                BlockType.I128Sparse -> TODO()
                BlockType.U128Sparse -> TODO()
            }
        }

//        if (type == BlockType.String) {
//            val len = buf.long.toInt()
//            (block as StringBlock).bytes = ByteArray(len)
//            buf.get(block.bytes, 0, len)
//        }

        return BlockHolder(type, block)
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