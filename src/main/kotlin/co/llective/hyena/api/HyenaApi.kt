package co.llective.hyena.api

import com.google.common.io.LittleEndianDataOutputStream
import io.airlift.log.Logger
import nanomsg.Nanomsg
import nanomsg.reqrep.ReqSocket
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.util.ArrayList

class HyenaApi {
    private val s = ReqSocket()

    var connected : Boolean = false
        private set

    @Synchronized
    private fun ensureConnected() {
        if (!connected) {
            throw RuntimeException("Hyena must be connected first!")
        }
    }

    @Throws(IOException::class)
    fun listColumns() : List<String> {
        val message = MessageBuilder.buildListColumnsMessage()
        s.send(message)
        val replyBuf = s.recv()
        val reply = MessageDecoder.decode(replyBuf);

        return when (reply) {
            is ListColumnsReply -> reply.columns.map { c -> c.name }
            else ->  {
                log.error("Got a wrong reply: " + reply)
                emptyList()
            }
        }
    }

    @Throws(IOException::class)
    fun connect(url: String) {
        log.info("Opening new connection to: " + url)
        s.setRecvTimeout(60000)
        s.setSendTimeout(60000)
        s.connect(url)
        log.info("Connection successfully opened")
        this.connected = true
    }

    fun close() {
        s.close()
    }

    @Throws(IOException::class)
    fun scan(req: ScanRequest, metaOrNull: HyenaOpMetadata?): ScanResult {
        ensureConnected()

        s.send(buildScanMessage(req))
        log.info("Sent scan request to partition " + req.partitionId)
        try {
            log.info("Waiting for scan response from partition " + req.partitionId)
            val buf = s.recv()
            log.info("Received scan response from partition " + req.partitionId)
            buf.order(ByteOrder.LITTLE_ENDIAN)

            val result = ScanResult.decode(buf)

            if (metaOrNull != null) {
                metaOrNull.bytes = buf.position()
            }

            return result
        } catch (t: Throwable) {
            log.error("Nanomsg error: " + Nanomsg.getError())
            throw IOException("Nanomsg error: " + Nanomsg.getError(), t)
        }

    }

    @Throws(IOException::class)
    fun refreshCatalog(): Catalog {
        ensureConnected()

        s.send(buildRefreshCatalogMessage())

        val buf = s.recv()
        buf.order(ByteOrder.LITTLE_ENDIAN)
        return decodeRefreshCatalog(buf)
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

    @Throws(IOException::class)
    internal fun buildScanMessage(req: ScanRequest): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeInt(ApiRequest.Scan.ordinal)

        val scanRequest = encodeScanRequest(req)
        baos.write(encodeByteArray(scanRequest))

        baos.close()

        return baos.toByteArray()
    }

    @Throws(IOException::class)
    internal fun encodeScanRequest(req: ScanRequest): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)

        dos.writeLong(req.minTs)
        dos.writeLong(req.maxTs)
        dos.writeLong(req.partitionId)

        dos.writeLong(req.projection.size.toLong())
        for (projectedColumn in req.projection) {
            dos.writeInt(projectedColumn)
        }

        if (req.filters == null) {
            dos.writeLong(0)
        } else {
            dos.writeLong(req.filters.size.toLong())
            for (filter in req.filters) {
                dos.write(encodeScanFilter(filter))
            }
        }

        return baos.toByteArray()
    }

    @Throws(IOException::class)
    internal fun encodeScanFilter(filter: ScanFilter): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)

        dos.writeInt(filter.column)
        dos.writeInt(filter.op.ordinal)
        dos.writeLong(filter.value)
        dos.write(encodeStringArray(filter.strValue))

        return baos.toByteArray()
    }

    @Throws(IOException::class)
    internal fun decodeRefreshCatalog(buf: ByteBuffer): Catalog {
        val columnCount = buf.long
        val columns = ArrayList<Column>()
        for (i in 0 until columnCount) {
            columns.add(MessageDecoder.decodeColumn(buf))
        }

        val partitionCount = buf.long
        val partitions = ArrayList<PartitionInfo>()
        for (i in 0 until partitionCount) {
            partitions.add(decodePartitionInfo(buf))
        }

        return Catalog(columns, partitions)
    }

    @Throws(IOException::class)
    internal fun decodePartitionInfo(buf: ByteBuffer): PartitionInfo {
        return PartitionInfo(buf.long, buf.long, buf.long, decodeStringArray(buf))
    }

    @Throws(IOException::class)
    internal fun encodeStringArray(str: String): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)

        val strBytes = str.toByteArray(UTF8_CHARSET)
        dos.writeLong(strBytes.size.toLong())
        dos.write(strBytes)

        return baos.toByteArray()
    }

    @Throws(IOException::class)
    internal fun decodeStringArray(buf: ByteBuffer): String {
        val len = buf.long.toInt()

        val bytes = ByteArray(len)
        buf.get(bytes, 0, len)
        return String(bytes, UTF8_CHARSET)
    }

    @Throws(IOException::class)
    internal fun encodeByteArray(values: ByteArray): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeLong(values.size.toLong())
        dos.write(values)

        //        byte[] bytes = baos.toByteArray();
        //        System.out.println("Sending: "+bytes.length);
        //        for (byte b:bytes) {
        //            System.out.println(b);
        //        }

        return baos.toByteArray()
    }

    protected fun finalize() {
        close()
    }

    companion object {
        private val log = Logger.get(HyenaApi::class.java)

        public val UTF8_CHARSET = Charset.forName("UTF-8")
    }

    class HyenaOpMetadata {
        var bytes: Int = 0
    }
}