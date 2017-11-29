package co.llective.hyena.api

import io.airlift.log.Logger
import nanomsg.Nanomsg
import nanomsg.Socket
import nanomsg.reqrep.ReqSocket
import java.io.IOException
import java.nio.charset.Charset
import java.util.*

open class HyenaApi internal constructor(private val connection: HyenaConnection){
    constructor() : this(HyenaConnection())

    @Throws(IOException::class)
    fun connect(address: String) = connection.connect(address)

    @Throws(IOException::class)
    fun close() = connection.finalize()

    private fun <T, C> makeApiCall(message: ByteArray, expected: Class<C>, extract: (C) -> T): T {
        val reply = connection.sendAndReceive(message)

        @Suppress("UNCHECKED_CAST")
        when (reply.javaClass) {
            expected -> return extract(reply as C)
            SerializeError::class.java -> {
                log.error("Serialization error: ${(reply as SerializeError).message}")
                throw ReplyException("Serialization error: ${reply.message}")
            }
            else -> {
                log.error("Got a wrong reply. Expected ${expected.simpleName}, got : $reply")
                throw ReplyException("Expected ${expected.simpleName}, got $reply")
            }
        }
    }

    @Throws(IOException::class, ReplyException::class)
    fun listColumns() : List<Column> {
        val message = MessageBuilder.buildListColumnsMessage()
        return makeApiCall(message, ListColumnsReply::class.java) { reply -> reply.columns }
    }

    @Throws(IOException::class, ReplyException::class)
    fun addColumn(column: Column) : Optional<Int> {
        val message = MessageBuilder.buildAddColumnMessage(column)
        return makeApiCall(message, AddColumnReply::class.java) {
            reply -> when (reply.result) {
                is Left -> Optional.of(reply.result.value)
                is Right -> {
                    log.error("Could not add column: ${reply.result.value}")
                    Optional.empty()
                }
            }
        }
    }

    @Throws(IOException::class, ReplyException::class)
    fun insert(source: Int, timestamps: List<Long>, vararg columnData: ColumnData) : Optional<Int> {
        val message = MessageBuilder.buildInsertMessage(source, timestamps, *columnData)
        return makeApiCall(message, InsertReply::class.java) { reply ->
            when (reply.result) {
                is Left -> Optional.of(reply.result.value)
                is Right -> {
                    log.error("Could not insert data ${reply.result.value}")
                    Optional.empty()
                }
            }
        }
    }

    @Throws(IOException::class, ReplyException::class)
    fun scan(req: ScanRequest, metaOrNull: HyenaOpMetadata?): ScanResult {
        val message = MessageBuilder.buildScanMessage(req)
        return makeApiCall(message, ScanReply::class.java) { reply ->
            when(reply.result) {
                is Left -> reply.result.value
                is Right -> {
                    throw ReplyException(reply.result.value)
                }
            }
        }
    }

    @Throws(IOException::class, ReplyException::class)
    fun refreshCatalog(): Catalog {
        val message = MessageBuilder.buildRefreshCatalogMessage()
        return makeApiCall(message, CatalogReply::class.java) { reply -> reply.result }
    }

    companion object {
        private val log = Logger.get(HyenaApi::class.java)

        val UTF8_CHARSET: Charset = Charset.forName("UTF-8")
    }

    class HyenaOpMetadata {
        var bytes: Int = 0
    }
}

open internal class HyenaConnection(private val s: Socket = ReqSocket(), private var connected: Boolean = false) {
    @Synchronized
    internal open fun ensureConnected() {
        if (!connected) {
            throw IOException("Hyena must be connected first!")
        }
    }

    @Throws(IOException::class, DeserializationException::class)
    open fun sendAndReceive(message: ByteArray): Reply {
        ensureConnected()

        try {
            s.send(message)
            val replyBuf = s.recv()
            return MessageDecoder.decode(replyBuf)
        } catch (exc: IOException) {
            throw IOException("Nanomsg error: " + Nanomsg.getError(), exc)
        }
    }

    @Throws(IOException::class)
    internal fun connect(url: String) {
        log.info("Opening new connection to: " + url)
        s.setRecvTimeout(60000)
        s.setSendTimeout(60000)
        s.connect(url)
        log.info("Connection successfully opened")
        this.connected = true
    }

    @Throws(IOException::class)
    internal fun finalize() {
        s.close()
    }

    companion object {
        private val log = Logger.get(HyenaConnection::class.java)
    }
}