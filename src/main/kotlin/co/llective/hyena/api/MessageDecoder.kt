package co.llective.hyena.api

import org.apache.commons.lang3.tuple.Pair
import java.io.IOException
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.*

object MessageDecoder {

    fun decode(buf: ByteBuffer) : Reply {
        try {
            val responseType = decodeMessageType(buf)

            return decodeMessage(responseType, buf)
        } catch (exc : RuntimeException) {
            throw DeserializationException(exc)
        }
    }

    internal fun decodeMessageType(buf: ByteBuffer) : ApiRequest {
        val messageTypeId = buf.int

        if (messageTypeId <= ApiRequest.values().size) {
            return ApiRequest.values()[messageTypeId]
        } else {
            throw throw DeserializationException("Cannot deserialize response type of index " + messageTypeId)
        }
    }

    private fun decodeMessage(request: ApiRequest, buf: ByteBuffer) : Reply {
        return when(request) {
            ApiRequest.ListColumns -> decodeListColumn(buf)
            ApiRequest.Insert -> InsertReply(decodeIntOrError(buf))
            ApiRequest.Scan -> ScanReply(decodeScanReplyOrError(buf))
            ApiRequest.RefreshCatalog -> CatalogReply(decodeRefreshCatalog(buf))
            ApiRequest.AddColumn -> AddColumnReply(decodeIntOrError(buf))
            ApiRequest.Flush -> TODO()
            ApiRequest.DataCompaction -> TODO()
            ApiRequest.Other -> TODO()
        }
    }

    @Throws(IOException::class)
    private fun decodeScanReplyOrError(buf: ByteBuffer): Either<ScanResult, ApiError> {
        val isError = buf.int
        return if (isError == 0) {
            val rowCount = buf.int
            val colCount = buf.int
            val columns = decodeColumnTypes(buf)
            val blocks = decodeBlocks(buf)

            Left(ScanResult(rowCount, colCount, columns, blocks))
        } else {
            Right(decodeApiError(buf))
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
        val uuidString = decodeString(buf)
        return PartitionInfo(minTs, maxTes, UUID.fromString(uuidString), decodeString(buf))
    }

    private fun decodeIntOrError(buf: ByteBuffer): Either<Int, ApiError> {
        val ok = buf.int
        return if (ok == 0) {
            Left(buf.long.toInt())
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

    private fun decodeListColumn(buf: ByteBuffer) : ListColumnsReply {
        val columnCount = buf.long.toInt()
        val columns = ArrayList<Column>()
        for (i in 0 until columnCount) {
            columns.add(decodeColumn(buf))
        }

        return ListColumnsReply(columns)
    }

    @Throws(IOException::class)
    private fun decodeColumn(buf: ByteBuffer): Column {
        val type = buf.int
        val id = buf.long.toInt()
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
    private fun decodeColumnTypes(buf: ByteBuffer): ArrayList<Pair<Int, BlockType>> {
        val colCount = buf.long
        val colTypes = ArrayList<Pair<Int, BlockType>>()

        for (i in 0 until colCount) {
            colTypes.add(Pair.of(buf.int, BlockType.values()[buf.int]))
        }

        return colTypes
    }

    @Throws(IOException::class)
    private fun decodeBlocks(buf: ByteBuffer): List<BlockHolder> {
        val count = buf.long
        val blocks = ArrayList<BlockHolder>()
        for (i in 0 until count) {
            blocks.add(decodeBlockHolder(buf))
        }
        return blocks
    }

    @Throws(IOException::class)
    private fun decodeBlockHolder(buf: ByteBuffer): BlockHolder {
        val recordsCount = buf.long.toInt()
        val type = BlockType.values()[buf.int]

        val block = when(type) {
            BlockType.String -> StringBlock(recordsCount)
            BlockType.I8Dense  -> DenseBlock<Byte>(type, recordsCount)
            BlockType.I16Dense -> DenseBlock<Short>(type, recordsCount)
            BlockType.I32Dense -> DenseBlock<Int>(type, recordsCount)
            BlockType.I64Dense -> DenseBlock<Long>(type, recordsCount)
            BlockType.U8Dense  -> DenseBlock<Short>(type, recordsCount)
            BlockType.U16Dense -> DenseBlock<Int>(type, recordsCount)
            BlockType.U32Dense -> DenseBlock<Long>(type, recordsCount)
            BlockType.U64Dense -> DenseBlock<BigInteger>(type, recordsCount)
            BlockType.I8Sparse  -> SparseBlock<Byte>(type, recordsCount)
            BlockType.I16Sparse -> SparseBlock<Short>(type, recordsCount)
            BlockType.I32Sparse -> SparseBlock<Int>(type, recordsCount)
            BlockType.I64Sparse -> SparseBlock<Long>(type, recordsCount)
            BlockType.U8Sparse  -> SparseBlock<Short>(type, recordsCount)
            BlockType.U16Sparse -> SparseBlock<Int>(type, recordsCount)
            BlockType.U32Sparse -> SparseBlock<Long>(type, recordsCount)
            BlockType.U64Sparse -> SparseBlock<BigInteger>(type, recordsCount)
        }

        for (i in 0 until recordsCount) {
            when (type) {
                BlockType.String -> (block as StringBlock).add(buf.int, buf.long)
                BlockType.I8Dense -> (block as DenseBlock<*>).add(buf.get())
                BlockType.I16Dense -> (block as DenseBlock<*>).add(buf.short)
                BlockType.I32Dense -> (block as DenseBlock<*>).add(buf.int)
                BlockType.I64Dense -> (block as DenseBlock<*>).add(buf.long)
                BlockType.U8Dense -> (block as DenseBlock<*>).add(buf.short)
                BlockType.U16Dense -> (block as DenseBlock<*>).add(buf.int)
                BlockType.U32Dense -> (block as DenseBlock<*>).add(buf.long)
                BlockType.U64Dense -> (block as DenseBlock<*>).add(decodeBigInt(buf.long))
                BlockType.I8Sparse -> (block as SparseBlock<*>).add(buf.int, buf.get())
                BlockType.I16Sparse -> (block as SparseBlock<*>).add(buf.int, buf.short)
                BlockType.I32Sparse -> (block as SparseBlock<*>).add(buf.int, buf.int)
                BlockType.I64Sparse -> (block as SparseBlock<*>).add(buf.int, buf.long)
                BlockType.U8Sparse -> (block as SparseBlock<*>).add(buf.int, buf.short)
                BlockType.U16Sparse -> (block as SparseBlock<*>).add(buf.int, buf.int)
                BlockType.U32Sparse -> (block as SparseBlock<*>).add(buf.int, buf.long)
                BlockType.U64Sparse -> (block as SparseBlock<*>).add(buf.int, decodeBigInt(buf.long))
            }
        }

//        if (type == BlockType.String) {
//            val len = buf.long.toInt()
//            (block as StringBlock).bytes = ByteArray(len)
//            buf.get(block.bytes, 0, len)
//        }

        return BlockHolder(type, block)
    }

    private val TWO_COMPLEMENT: BigInteger = BigInteger.ONE.shiftLeft(64);

    internal fun decodeBigInt(value: Long): BigInteger {
        var bi = BigInteger.valueOf(value)
        if (bi < BigInteger.ZERO) {
            bi += TWO_COMPLEMENT
        }
        return bi
    }
}

class DeserializationException : Exception {
    constructor(msg : String) : super(msg)
    constructor(msg : String, cause : Exception) : super(msg, cause)
    constructor(cause : Exception) : this("Error during deserialization occurred", cause)
}