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
    internal fun decodeBlockHolder(buf: ByteBuffer): BlockHolder {
        val type = BlockType.values()[buf.int]
        val recordsCount = buf.long.toInt()

        val block = when (type) {
            BlockType.I8Dense -> fillDenseBlock(DenseBlock<Byte>(type, recordsCount), recordsCount, buf)
            BlockType.I16Dense -> fillDenseBlock(DenseBlock<Short>(type, recordsCount), recordsCount, buf)
            BlockType.I32Dense -> fillDenseBlock(DenseBlock<Int>(type, recordsCount), recordsCount, buf)
            BlockType.I64Dense -> fillDenseBlock(DenseBlock<Long>(type, recordsCount), recordsCount, buf)
            BlockType.U8Dense -> fillDenseBlock(DenseBlock<Short>(type, recordsCount), recordsCount, buf)
            BlockType.U16Dense -> fillDenseBlock(DenseBlock<Int>(type, recordsCount), recordsCount, buf)
            BlockType.U32Dense -> fillDenseBlock(DenseBlock<Long>(type, recordsCount), recordsCount, buf)
            BlockType.U64Dense -> fillDenseBlock(DenseBlock<BigInteger>(type, recordsCount), recordsCount, buf)
            BlockType.I8Sparse -> fillSparseBlock(SparseBlock<Byte>(type, recordsCount), recordsCount, buf)
            BlockType.I16Sparse -> fillSparseBlock(SparseBlock<Short>(type, recordsCount), recordsCount, buf)
            BlockType.I32Sparse -> fillSparseBlock(SparseBlock<Int>(type, recordsCount), recordsCount, buf)
            BlockType.I64Sparse -> fillSparseBlock(SparseBlock<Long>(type, recordsCount), recordsCount, buf)
            BlockType.U8Sparse -> fillSparseBlock(SparseBlock<Short>(type, recordsCount), recordsCount, buf)
            BlockType.U16Sparse -> fillSparseBlock(SparseBlock<Int>(type, recordsCount), recordsCount, buf)
            BlockType.U32Sparse -> fillSparseBlock(SparseBlock<Long>(type, recordsCount), recordsCount, buf)
            BlockType.U64Sparse -> fillSparseBlock(SparseBlock<BigInteger>(type, recordsCount), recordsCount, buf)
            BlockType.String -> TODO("Strings are not supported yet")
            else -> {
                // 128 bit
                TODO("implement")
            }
        }

        return BlockHolder(type, block)
    }

    private fun <T: Number> fillDenseBlock(denseBlock: DenseBlock<T>, vectorLen: Int, buf: ByteBuffer): DenseBlock<T> {
        for (i in 0 until vectorLen) {
            when (denseBlock.type) {
                BlockType.I8Dense -> (denseBlock as DenseBlock<Byte>).add(buf.get())
                BlockType.I16Dense -> (denseBlock as DenseBlock<Short>).add(buf.short)
                BlockType.I32Dense -> (denseBlock as DenseBlock<Int>).add(buf.int)
                BlockType.I64Dense -> (denseBlock as DenseBlock<Long>).add(buf.long)
                BlockType.U8Dense -> (denseBlock as DenseBlock<Short>).add(buf.get().toShort())
                BlockType.U16Dense -> (denseBlock as DenseBlock<Int>).add(buf.short.toInt())
                BlockType.U32Dense -> (denseBlock as DenseBlock<Long>).add(buf.int.toLong())
                BlockType.U64Dense -> (denseBlock as DenseBlock<BigInteger>).add(decodeBigInt(buf.long))
                BlockType.I128Dense -> TODO()
                BlockType.U128Dense -> TODO()
                else -> throw DeserializationException("Sparse/String block cannot be serialized as dense one")
            }
        }

        return denseBlock
    }

    private fun <T: Number> fillSparseBlock(sparseBlock: SparseBlock<T>, vectorsLen: Int, buf: ByteBuffer): SparseBlock<T> {
        val type = sparseBlock.type

        // deserialize values vector (size is already taken from buffer)
        val valueList: MutableList<T> = ArrayList(vectorsLen)
        for (i in 0 until vectorsLen) {
            when (type) {
                BlockType.I8Sparse -> valueList.add(buf.get() as T)
                BlockType.I16Sparse -> valueList.add(buf.short as T)
                BlockType.I32Sparse -> valueList.add(buf.int as T)
                BlockType.I64Sparse -> valueList.add(buf.long as T)
                BlockType.U8Sparse -> valueList.add(buf.get().toShort() as T)
                BlockType.U16Sparse -> valueList.add(buf.short.toInt() as T)
                BlockType.U32Sparse -> valueList.add(buf.int.toLong() as T)
                BlockType.U64Sparse -> valueList.add(decodeBigInt(buf.long) as T)
                BlockType.I128Sparse -> TODO()
                BlockType.U128Sparse -> TODO()
                else -> throw DeserializationException("Dense/String block cannot be serialized as sparse one")
            }
        }

        // deserialize offsets vector
        if (buf.long.toInt() != vectorsLen) {
            throw DeserializationException("Sparse data inconsistent, values len doesn't match offsets len")
        }
        val offsetList: MutableList<Int> = ArrayList(vectorsLen)
        for (i in 0 until vectorsLen) {
            offsetList.add(buf.int)
        }

        // add vectors to sparse block
        for (i in 0 until vectorsLen) {
            sparseBlock.add(offsetList[i], valueList[i])
        }

        return sparseBlock
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