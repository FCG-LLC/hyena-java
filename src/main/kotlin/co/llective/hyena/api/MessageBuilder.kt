package co.llective.hyena.api

import com.google.common.io.LittleEndianDataOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutput
import java.io.IOException
import java.math.BigInteger
import java.util.UUID

object MessageBuilder {

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

    fun buildInsertMessage(source: Int, timestamps: List<Long>, vararg columnData: ColumnData): ByteArray {
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

        if (req.partitionIds != null) {
            dos.writeBoolean(true)
            val partitionIds = req.partitionIds!!
            dos.writeLong(partitionIds.size.toLong())
            for (i in 0 until partitionIds.size) {
                writeUUID(dos, partitionIds.elementAt(i))
            }
        } else {
            dos.writeBoolean(false)
        }

        writeLongList(dos, req.projection)

        dos.writeLong(req.filters.size.toLong())
        for (filter in req.filters) {
            dos.write(encodeScanFilter(filter))
        }

        baos.close()

        return baos.toByteArray()
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
        if (filter.strValue.isPresent) {
            writeString(dos, filter.strValue.get())
        } else {
            dos.writeLong(0) // Length of string: 0
        }
        baos.close()

        return baos.toByteArray()
    }

    private val TWO_COMPLEMENT: BigInteger = BigInteger.ONE.shiftLeft(64)
    private val MAX_LONG_BI: BigInteger = BigInteger.valueOf(Long.MAX_VALUE)

    internal fun writeU64(dos: DataOutput, value: BigInteger) {
        val bi = if (value <= MAX_LONG_BI) {value} else { value - TWO_COMPLEMENT }
        dos.writeLong(bi.longValueExact())
    }

    private fun writeFilterValue(dos: DataOutput, filter: ScanFilter) {
        dos.writeInt(filter.type.ordinal)
        when(filter.type) {
            FilterType.I8  -> dos.writeByte(filter.value as Int)
            FilterType.I16 -> dos.writeShort(filter.value as Int)
            FilterType.I32 -> dos.writeInt(filter.value as Int)
            FilterType.I64 -> dos.writeLong(filter.value as Long)

            FilterType.U8  -> dos.writeShort(filter.value as Int)
            FilterType.U16 -> dos.writeInt(filter.value as Int)
            FilterType.U32 -> dos.writeLong(filter.value as Long)
            FilterType.U64 -> writeU64(dos, BigInteger.valueOf(filter.value as Long))

            FilterType.String -> TODO()
        }
    }
}