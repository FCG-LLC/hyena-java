package co.llective.hyena.api

import com.google.common.io.LittleEndianDataOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutput
import java.io.IOException
import java.math.BigInteger
import java.util.*

object MessageBuilder {

    fun buildConnectMessage(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)

        // ControlRequest enum, now has only one value - CreateSocket
        dos.writeInt(0)

        baos.close()
        return baos.toByteArray()
    }

    fun buildKeepAliveRequest(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)

        // ControlRequest enum, now has only one value - CreateSocket
        dos.writeInt(PeerRequestType.KeepAlive.ordinal)

        baos.close()
        return baos.toByteArray()
    }

    fun wrapRequestIntoPeerRequest(requestType: PeerRequestType, messageId: Long, message: ByteArray? = null): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)

        val typeId = PeerRequestType.values().indexOf(requestType)
        dos.writeInt(typeId)

        when (requestType) {
            PeerRequestType.Request -> {
                dos.writeLong(messageId)
                if (message!!.isEmpty()) {
                    //option=false
                    dos.write(0)
                } else {
                    //option=true
                    dos.write(1)
                    dos.writeLong(message.size.toLong())
                    dos.write(message)
                }
            }
            PeerRequestType.Abort -> {
                dos.writeLong(messageId)
            }
            PeerRequestType.CloseConnection, PeerRequestType.KeepAlive -> {
            } //no need for additional payload
        }

        baos.close()
        return baos.toByteArray()
    }

    @Throws(IOException::class)
    fun buildListColumnsMessage(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeInt(ApiRequest.ListColumns.ordinal)
        baos.close()

        return baos.toByteArray()
    }

    fun buildAddColumnMessage(column: Column): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeInt(ApiRequest.AddColumn.ordinal)
        writeString(dos, column.name)
        dos.writeInt(column.dataType.ordinal)
        baos.close()

        return baos.toByteArray()
    }

    fun buildInsertMessage(source: Int, timestamps: List<Long>, columnData: List<ColumnData>): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeInt(ApiRequest.Insert.ordinal)
        writeLongList(dos, timestamps)
        dos.writeInt(source)

        dos.writeLong(columnData.size.toLong())
        for (column in columnData) {
            writeColumn(dos, column)
        }
        baos.close()

        return baos.toByteArray()
    }

    @Throws(IOException::class)
    fun buildScanMessage(req: ScanRequest): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeInt(ApiRequest.Scan.ordinal)

        dos.writeLong(req.minTs)
        dos.writeLong(req.maxTs)

        writeUUIDCollection(dos, req.partitionIds);

        writeLongList(dos, req.projection)

        dos.writeLong(req.filters.size.toLong())
        for (andFilters in req.filters) {
            dos.writeLong(andFilters.size.toLong())
            for (filter in andFilters) {
                dos.write(encodeScanFilter(filter))
            }
        }

        writeScanConfig(dos, req.scanConfig)

        baos.close()
        return baos.toByteArray()
    }

    private fun writeScanConfig(dos: DataOutput, scanConfig: Optional<StreamConfig>) {
        if (!scanConfig.isPresent) {
            dos.writeBoolean(false)
            return
        }
        dos.writeBoolean(true)
        val config = scanConfig.get()
        dos.writeLong(config.limit)
        dos.writeLong(config.threshold)
        if (!config.streamState.isPresent) {
            dos.writeBoolean(false)
            return
        }
        dos.writeBoolean(true)
        val streamState = config.streamState.get()
        dos.writeLong(streamState.skipChunks)
    }

    private fun writeUUIDCollection(dos: DataOutput, partitionIds: Set<UUID>) {
        dos.writeLong(partitionIds.size.toLong())
        for (partitionId in partitionIds) {
            writeUUID(dos, partitionId)
        }
    }

    private fun <T> writeNullable(dos: DataOutput, nullable: T?, writeContent: (DataOutput, T) -> Unit) {
        if (nullable != null) {
            dos.writeBoolean(true)
            writeContent(dos, nullable)
        } else {
            dos.writeBoolean(false)
        }
    }

    @Throws(IOException::class)
    private fun writeUUID(dos: DataOutput, uuid: UUID) {
        dos.writeLong(uuid.mostSignificantBits)
        dos.writeLong(uuid.leastSignificantBits)
    }

    @Throws(IOException::class)
    fun buildRefreshCatalogMessage(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeInt(ApiRequest.RefreshCatalog.ordinal)
        baos.close()

        return baos.toByteArray()
    }

    private fun writeColumn(dos: DataOutput, column: ColumnData) {
        dos.writeLong(1) // Each ColumnData will become a single-item BlockData in Rust
        dos.writeLong(column.columnIndex.toLong())
        dos.writeInt(column.block.type.ordinal)
        column.block.write(dos)
    }

    private fun writeLongList(dos: DataOutput, list: List<Long>) {
        dos.writeLong(list.size.toLong())
        for (item in list) {
            dos.writeLong(item)
        }
    }

    private fun writeIntList(dos: DataOutput, list: List<Int>) {
        dos.writeLong(list.size.toLong())
        for (item in list) {
            dos.writeInt(item)
        }
    }

    private fun writeString(stream: DataOutput, string: String) {
        val bytes = string.toByteArray(HyenaApi.UTF8_CHARSET)
        stream.writeLong(bytes.size.toLong())
        stream.write(bytes)
    }

    @Throws(IOException::class)
    private fun encodeScanFilter(filter: ScanFilter): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)

        dos.writeLong(filter.column)
        dos.writeInt(filter.op.ordinal)
        writeFilterValue(dos, filter)

        baos.close()

        return baos.toByteArray()
    }

    private val TWO_COMPLEMENT: BigInteger = BigInteger.ONE.shiftLeft(64)
    private val MAX_LONG_BI: BigInteger = BigInteger.valueOf(Long.MAX_VALUE)

    internal fun writeU64(dos: DataOutput, value: BigInteger) {
        val bi = if (value <= MAX_LONG_BI) {
            value
        } else {
            value - TWO_COMPLEMENT
        }
        dos.writeLong(bi.longValueExact())
    }

    private fun writeFilterValue(dos: DataOutput, filter: ScanFilter) {
        dos.writeInt(filter.type.ordinal)
        when (filter.type) {
            FilterType.I8 -> dos.writeByte((filter.value as Long).toInt())
            FilterType.I16 -> dos.writeShort((filter.value as Long).toInt())
            FilterType.I32 -> dos.writeInt((filter.value as Long).toInt())
            FilterType.I64 -> dos.writeLong(filter.value as Long)
            FilterType.I128 -> TODO()

            FilterType.U8 -> dos.writeByte((filter.value as Long).toInt())
            FilterType.U16 -> dos.writeShort((filter.value as Long).toInt())
            FilterType.U32 -> dos.writeInt((filter.value as Long).toInt())
            FilterType.U64 -> dos.writeLong(filter.value as Long)
            FilterType.U128 -> TODO()

            FilterType.String -> writeString(dos, filter.value as String)
        }
    }
}