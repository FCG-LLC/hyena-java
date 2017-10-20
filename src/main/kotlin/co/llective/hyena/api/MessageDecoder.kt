package co.llective.hyena.api

import org.apache.commons.lang3.tuple.Pair
import java.io.IOException
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.*

object MessageDecoder {
    fun decode(buf: ByteBuffer) : Reply {
        val requestType = ApiRequest.values()[buf.int];

        return decode(requestType, buf)
    }

    fun decode(request: ApiRequest, buf: ByteBuffer) : Reply {
        return when(request) {
            ApiRequest.ListColumns -> decodeListColumn(buf)
            ApiRequest.Insert -> InsertReply(decodeEither(buf))
            ApiRequest.Scan -> ScanReply(decodeScanReply(buf))
            ApiRequest.RefreshCatalog -> TODO()
            ApiRequest.AddColumn -> AddColumnReply(decodeEither(buf))
            ApiRequest.Flush -> TODO()
            ApiRequest.DataCompaction -> TODO()
        }
    }

    @Throws(IOException::class)
    fun decodeScanReply(buf: ByteBuffer): ScanResult {
        val type = buf.int
        // TODO validate message type
        val rowCount = buf.int
        val colCount = buf.int

        return ScanResult(
                rowCount,
                colCount,
                decodeColumnTypes(buf),
                decodeBlocks(buf)
        )
    }

    private fun decodeEither(buf: ByteBuffer): Either<Int, ApiError> {
        val ok = buf.int
        if (ok == 0) {
            return Left(buf.long.toInt())
        } else {
            return Right(decodeApiError(buf))
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
    internal fun decodeColumn(buf: ByteBuffer): Column {
        val type = buf.int
        val id = buf.long.toInt()
        return Column(BlockType.values()[type], id, decodeString(buf))
    }

    @Throws(IOException::class)
    internal fun decodeString(buf: ByteBuffer): String {
        val len = buf.long.toInt()

        val bytes = ByteArray(len)
        buf.get(bytes, 0, len)
        return String(bytes, HyenaApi.UTF8_CHARSET)
    }

    @Throws(IOException::class)
    private fun decodeColumnTypes(buf: ByteBuffer): List<Pair<Int, BlockType>> {
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
    fun decodeBlockHolder(buf: ByteBuffer): BlockHolder {
        val holder = BlockHolder()
        holder.type = BlockType.values()[buf.int]
        val recordsCount = buf.long.toInt()

        when (holder.type) {
            BlockType.String -> holder.block = Optional.of(StringBlock(recordsCount))
            BlockType.I8Dense  -> holder.block = Optional.of(DenseBlock<Byte>(holder.type!!, recordsCount))
            BlockType.I16Dense -> holder.block = Optional.of(DenseBlock<Short>(holder.type!!, recordsCount))
            BlockType.I32Dense -> holder.block = Optional.of(DenseBlock<Int>(holder.type!!, recordsCount))
            BlockType.I64Dense -> holder.block = Optional.of(DenseBlock<Long>(holder.type!!, recordsCount))
            BlockType.U8Dense  -> holder.block = Optional.of(DenseBlock<Short>(holder.type!!, recordsCount))
            BlockType.U16Dense -> holder.block = Optional.of(DenseBlock<Int>(holder.type!!, recordsCount))
            BlockType.U32Dense -> holder.block = Optional.of(DenseBlock<Long>(holder.type!!, recordsCount))
            BlockType.U64Dense -> holder.block = Optional.of(DenseBlock<BigInteger>(holder.type!!, recordsCount))
            BlockType.I8Sparse  -> holder.block = Optional.of(SparseBlock<Byte>(holder.type!!, recordsCount))
            BlockType.I16Sparse -> holder.block = Optional.of(SparseBlock<Short>(holder.type!!, recordsCount))
            BlockType.I32Sparse -> holder.block = Optional.of(SparseBlock<Int>(holder.type!!, recordsCount))
            BlockType.I64Sparse -> holder.block = Optional.of(SparseBlock<Long>(holder.type!!, recordsCount))
            BlockType.U8Sparse  -> holder.block = Optional.of(SparseBlock<Short>(holder.type!!, recordsCount))
            BlockType.U16Sparse -> holder.block = Optional.of(SparseBlock<Int>(holder.type!!, recordsCount))
            BlockType.U32Sparse -> holder.block = Optional.of(SparseBlock<Long>(holder.type!!, recordsCount))
            BlockType.U64Sparse -> holder.block = Optional.of(SparseBlock<BigInteger>(holder.type!!, recordsCount))
        }

        for (i in 0 until recordsCount) {
            when (holder.type) {
                BlockType.String -> (holder.block.get() as StringBlock).add(buf.int, buf.long)
                BlockType.I8Dense -> (holder.block.get() as DenseBlock<*>).add(buf.get())
                BlockType.I16Dense -> (holder.block.get() as DenseBlock<*>).add(buf.short)
                BlockType.I32Dense -> (holder.block.get() as DenseBlock<*>).add(buf.int)
                BlockType.I64Dense -> (holder.block.get() as DenseBlock<*>).add(buf.long)
                BlockType.U8Dense -> TODO()
                BlockType.U16Dense -> TODO()
                BlockType.U32Dense -> TODO()
                BlockType.U64Dense -> TODO()
                BlockType.I8Sparse -> (holder.block.get() as SparseBlock<*>).add(buf.int, buf.get())
                BlockType.I16Sparse -> (holder.block.get() as SparseBlock<*>).add(buf.int, buf.short)
                BlockType.I32Sparse -> (holder.block.get() as SparseBlock<*>).add(buf.int, buf.int)
                BlockType.I64Sparse -> (holder.block.get() as SparseBlock<*>).add(buf.int, buf.long)
                BlockType.U8Sparse -> TODO()
                BlockType.U16Sparse -> TODO()
                BlockType.U32Sparse -> TODO()
                BlockType.U64Sparse -> TODO()
                null -> TODO()
            }
        }

        if (holder.type == BlockType.String) {
            val len = buf.long.toInt()
            val block = holder.block.get() as StringBlock
            block.bytes = ByteArray(len)
            buf.get(block.bytes, 0, len)
        }

        return holder
    }
}