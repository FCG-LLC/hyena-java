package co.llective.hyena.api

import io.airlift.log.Logger
import nanomsg.Nanomsg
import nanomsg.reqrep.ReqSocket
import java.io.IOException
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.util.*

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
    fun listColumns() : List<Column> {
        val message = MessageBuilder.buildListColumnsMessage()
        s.send(message)
        val replyBuf = s.recv()
        val reply = MessageDecoder.decode(replyBuf);

        return when (reply) {
            is ListColumnsReply -> reply.columns
            else ->  {
                log.error("Got a wrong reply: " + reply)
                emptyList()
            }
        }
    }

    @Throws(IOException::class)
    fun addColumn(column: Column) : Optional<Int> {
        val message = MessageBuilder.buildAddColumnMessage(column)
        s.send(message)
        val replyBuf = s.recv()
        val reply = MessageDecoder.decode(replyBuf);

        return when (reply) {
            is AddColumnReply -> when (reply.result) {
                is Left -> Optional.of(reply.result.value)
                is Right -> {
                    log.error("Could not add column: ${reply.result.value}");
                    Optional.empty()
                }
            }
            else -> {
                log.error("Got a wrong reply: " + reply)
                Optional.empty()
            }
        }
    }

    @Throws(IOException::class)
    fun insert(source: Int, timestamps: List<Long>, vararg columnData: ColumnData) : Optional<Int> {
        val message = MessageBuilder.buildInsertMessage(source, timestamps, *columnData)
        s.send(message)
        val replyBuf = s.recv()
        val reply = MessageDecoder.decode(replyBuf);

        return when (reply) {
            is InsertReply -> when (reply.result) {
                is Left -> Optional.of(reply.result.value)
                is Right -> {
                    log.error("Could not insert data");
                    Optional.empty()
                }
            }
            else -> {
                log.error("Got a wrong reply: " + reply)
                Optional.empty()
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

        s.send(MessageBuilder.buildScanMessage(req))
        log.info("Sent scan request to partition " + req.partitionId)
        try {
            log.info("Waiting for scan response from partition " + req.partitionId)
            val buf = s.recv()
            log.info("Received scan response from partition " + req.partitionId)
            buf.order(ByteOrder.LITTLE_ENDIAN)

            val result = MessageDecoder.decodeScanReply(buf)

            metaOrNull?.bytes = buf.position()

            return result
        } catch (t: Throwable) {
            log.error("Nanomsg error: " + Nanomsg.getError())
            throw IOException("Nanomsg error: " + Nanomsg.getError(), t)
        }
    }

    @Throws(IOException::class)
    fun refreshCatalog(): Catalog {
        ensureConnected()

        s.send(MessageBuilder.buildRefreshCatalogMessage())

        val buf = s.recv()
        buf.order(ByteOrder.LITTLE_ENDIAN)
        return MessageDecoder.decodeRefreshCatalog(buf)
    }

    protected fun finalize() {
        close()
    }

    companion object {
        private val log = Logger.get(HyenaApi::class.java)

        val UTF8_CHARSET = Charset.forName("UTF-8")
    }

    class HyenaOpMetadata {
        var bytes: Int = 0
    }
}