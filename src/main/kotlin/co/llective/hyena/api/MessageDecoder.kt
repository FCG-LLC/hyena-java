package co.llective.hyena.api

import java.io.IOException
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
            ApiRequest.Insert -> TODO()
            ApiRequest.Scan -> TODO()
            ApiRequest.RefreshCatalog -> TODO()
            ApiRequest.AddColumn -> AddColumnReply(decodeAddColumnReply(buf))
            ApiRequest.Flush -> TODO()
            ApiRequest.DataCompaction -> TODO()
        }
    }

    private fun decodeAddColumnReply(buf: ByteBuffer): Either<Int, ApiError> {
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
}