package co.llective.hyena.api

import co.llective.hyena.PeerConnection
import co.llective.hyena.PeerConnectionManager
import io.airlift.log.Logger
import nanomsg.exceptions.EAgainException
import java.io.IOException
import java.lang.Thread.sleep
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.schedule

open class HyenaApi internal constructor(private val connection: ConnectionManager) {
    constructor(address: String) : this(ConnectionManager(address))

    private fun <T, C> makeApiCall(message: ByteArray, expected: Class<C>, extract: (C) -> T): T {
        val replyFuture = connection.sendRequest(message)
        sleep(Random().nextDouble().times(1000).toLong())
        val reply = replyFuture.get()

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
    fun listColumns(): List<Column> {
        val message = MessageBuilder.buildListColumnsMessage()
        return makeApiCall(message, ListColumnsReply::class.java) { reply -> reply.columns }
    }

    @Throws(IOException::class, ReplyException::class)
    fun addColumn(column: Column): Optional<Int> {
        val message = MessageBuilder.buildAddColumnMessage(column)
        return makeApiCall(message, AddColumnReply::class.java) { reply ->
            when (reply.result) {
                is Left -> Optional.of(reply.result.value)
                is Right -> {
                    log.error("Could not add column: ${reply.result.value}")
                    Optional.empty()
                }
            }
        }
    }

    @Throws(IOException::class, ReplyException::class)
    fun insert(source: Int, timestamps: List<Long>, vararg columnData: ColumnData): Optional<Int> {
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
    fun scan(req: ScanRequest): ScanResult {
        val message = MessageBuilder.buildScanMessage(req)
        return makeApiCall(message, ScanReply::class.java) { reply ->
            when (reply.result) {
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
}

class ConnectionManager {
    private val hyenaAddress: String
    private var connectionManager: PeerConnectionManager
    private var connection: PeerConnection
    private val requests : ConcurrentHashMap<Long, CompletableFuture<Any>> = ConcurrentHashMap()
    private val keepAliveResponse: AtomicBoolean = AtomicBoolean(true)

    constructor(hyenaAddress: String) {
        this.hyenaAddress = hyenaAddress
        this.connectionManager = PeerConnectionManager(hyenaAddress)
        this.connection = connectionManager.getPeerConnection()
        Timer().schedule(
                0,
                10 * 1000,
                {
                    try {
                        if (keepAliveResponse.get()) {
                            sendKeepAlive()
                        } else {
                            log.error("Resetting connection - no keepalive response")
                            connection.close()
                            connection = connectionManager.getPeerConnection()
                            keepAliveResponse.set(true)
                        }
                    } catch(exc: nanomsg.exceptions.IOException) {
                        keepAliveResponse.set(false)
                    }
                }
        )
        Timer().schedule(
                500,
                200,
                {
                    try {
                        receiveData()
                    } catch(exc: EAgainException) {
                        // It means no message was waiting in nanomsg and we'll try later
                    }
                }
        )
    }

    private fun sendKeepAlive() {
        val bytes = MessageBuilder.buildKeepAliveRequest()
        connection.synchronizedReq(bytes)

    }

    fun sendRequest(message: ByteArray) : Future<*> {
        val messageId = UUID.randomUUID().leastSignificantBits
        val future = CompletableFuture<Any>()
        requests[messageId] = future
        connection.synchronizedReq(request = MessageBuilder.wrapRequestIntoPeerRequest(PeerRequestType.Request, messageId, message))
        return future
    }

    private fun receiveData() {
        val replyBuf = connection.getSynRespBufferNoWait()
        val reply = MessageDecoder.decodePeerReply(replyBuf)
        when(reply) {
            is KeepAliveReply -> keepAliveResponse.set(true)
            is ResponseReply -> {
                val future = requests.remove(reply.messageId)
                if (future == null) {
                    throw Exception("No message of id ${reply.messageId} found in registry")
                } else {
                    future.complete(MessageDecoder.decode(reply.bufferPayload))
                }
            }
            is ResponseReplyError -> {
                val future = requests.remove(reply.messageId)
                if (future == null) {
                    throw Exception("No message of id ${reply.messageId} found in registry")
                } else {
                    future.completeExceptionally(IOException("Hyena: PeerReplyError for message: ${reply.messageId}"))
                }
            }
        }
    }

    companion object {
        private val log = Logger.get(ConnectionManager::class.java)
    }
}
