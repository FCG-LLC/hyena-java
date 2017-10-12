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
            ApiRequest.AddColumn -> AddColumnReply()
            ApiRequest.Flush -> TODO()
            ApiRequest.DataCompaction -> TODO()
        }
    }

    private fun decodeListColumn(buf: ByteBuffer) : ListColumnsReply {
        val columnCount = buf.int
        val columns = ArrayList<Column>()
        for (i in 0 until columnCount) {
            columns.add(decodeColumn(buf))
        }

        return ListColumnsReply(columns)
    }

    @Throws(IOException::class)
    internal fun decodeColumn(buf: ByteBuffer): Column {
        val mem = buf.int // Consume wrapper type Memory
        val type = buf.int
        return Column(BlockType.values()[type], decodeString(buf))
    }

    @Throws(IOException::class)
    internal fun decodeString(buf: ByteBuffer): String {
        val len = buf.long.toInt()

        val bytes = ByteArray(len)
        buf.get(bytes, 0, len)
        return String(bytes, HyenaApi.UTF8_CHARSET)
    }
}