package co.llective.hyena.api

import com.google.common.io.LittleEndianDataOutputStream
import java.io.*

object MessageBuilder {

    @Throws(IOException::class)
    internal fun buildListColumnsMessage(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeInt(ApiRequest.ListColumns.ordinal)
        dos.writeLong(0L) // 0 bytes for payload
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
        dos.writeLong(req.partitionId)

        writeIntList(dos, req.projection)

        dos.writeLong(req.filters.size.toLong())
        for (filter in req.filters) {
            dos.write(encodeScanFilter(filter))
        }

        baos.close()

        return baos.toByteArray()
    }

    @Throws(IOException::class)
    internal fun buildRefreshCatalogMessage(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeInt(ApiRequest.RefreshCatalog.ordinal)
        dos.writeLong(0L) // 0 bytes for payload
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

        dos.writeInt(filter.column)
        dos.writeInt(filter.op.ordinal)
        dos.writeLong(filter.value)
        writeString(dos, filter.strValue)

        return baos.toByteArray()
    }
}