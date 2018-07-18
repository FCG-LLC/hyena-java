package co.llective.hyena.api

import co.llective.hyena.PeerConnection
import co.llective.hyena.PeerConnectionManager
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import io.airlift.log.Logger
import nanomsg.Nanomsg
import nanomsg.exceptions.EAgainException
import java.io.IOException
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.schedule

open class HyenaApi private constructor(private val connection: ConnectionManager,
                                        private val catalogRefresh: Long) {

    private constructor(builder: Builder) : this(builder.connectionManager!!, builder.catalogRefresh)

    class Builder {
        var address: String? = null
            private set
        var connectionManager: ConnectionManager? = null
            private set
        var catalogRefresh: Long = CATALOG_CACHE_TIMEOUT_MS
            private set
        var nanomsgPullIntervalMs: Long = NANOMSG_PULL_INTERVAL_MS
            private set

        fun address(address: String) = apply { this.address = address }
        fun connection(connectionManager: ConnectionManager) = apply { this.connectionManager = connectionManager }
        fun catalogRefresh(catalogRefresh: Long) = apply { this.catalogRefresh = catalogRefresh }
        fun nanomsgPullIntervalMs(nanomsgPullIntervalMs: Long) = apply { this.nanomsgPullIntervalMs = nanomsgPullIntervalMs }
        fun build(): HyenaApi {
            if (this.connectionManager == null && this.address == null) {
                throw IllegalStateException("address or connection manager has to be specified in order to build HyenaApi")
            }
            this.connectionManager = if (this.connectionManager != null) this.connectionManager else ConnectionManager(address!!, nanomsgPullIntervalMs)
            return HyenaApi(this)
        }
    }

    private val catalogCache: LoadingCache<Int, Catalog> = CacheBuilder.newBuilder()
            .maximumSize(1)
            .expireAfterWrite(catalogRefresh, TimeUnit.MILLISECONDS)
            .build(object : CacheLoader<Int, Catalog>() {
                override fun load(key: Int): Catalog {
                    log.info("Refreshing catalog")
                    val message = MessageBuilder.buildRefreshCatalogMessage()
                    val catalog = makeApiCall(message, CatalogReply::class.java) { reply -> reply.result }
                    val sortedCatalog = Catalog(
                            catalog.columns.sortedBy { x -> x.id },
                            catalog.availablePartitions
                    )
                    return sortedCatalog
                }
            })

    private fun <T, C> makeApiCall(message: ByteArray, expected: Class<C>, extract: (C) -> T): T {
        val replyFuture = connection.sendRequest(message)
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
    fun insert(source: Int, timestamps: List<Long>, columnData: List<ColumnData>): Optional<Int> {
        val message = MessageBuilder.buildInsertMessage(source, timestamps, columnData)
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
                is Left ->
                    reply.result.value
                is Right -> {
                    throw ReplyException(reply.result.value)
                }
            }
        }
    }

    @Throws(IOException::class, ReplyException::class)
    fun refreshCatalog(): Catalog = refreshCatalog(false)

    @Throws(IOException::class, ReplyException::class)
    fun refreshCatalog(force: Boolean = false): Catalog =
            try {
                if (force) {
                    catalogCache.invalidate(DUMMY_CACHE_KEY)
                }
                catalogCache[DUMMY_CACHE_KEY]
            } catch (e: ExecutionException) {
                throw e.cause ?: ReplyException("Unknown exception")
            }

    /**
     * Cleans all resources attached to connection.
     */
    fun purge() {
        connection.shutDown()
    }

    companion object {
        private val log = Logger.get(HyenaApi::class.java)

        val UTF8_CHARSET: Charset = Charset.forName("UTF-8")
        private const val CATALOG_CACHE_TIMEOUT_MS: Long = 5 * 60 * 1000 // 5 minutes
        private const val NANOMSG_PULL_INTERVAL_MS: Long = 50
        private const val DUMMY_CACHE_KEY: Int = 0x0BCABABA
    }
}

private const val RECEIVE_START_DELAY_MS: Long = 500L       //500ms
private const val KEEP_ALIVE_START_DELAY_MS: Long = 0L
private const val KEEP_ALIVE_INTERVAL_MS: Long = 10 * 1000L //10s

open class ConnectionManager {
    private val hyenaAddress: String
    private var connectionManager: PeerConnectionManager
    private var connection: PeerConnection
    private val requests: ConcurrentHashMap<Long, CompletableFuture<Any>> = ConcurrentHashMap()
    internal val keepAliveResponse: AtomicBoolean = AtomicBoolean(true)
    private val receiveScheduler = Timer()
    private val keepAliveScheduler = Timer()

    internal constructor(hyenaAddress: String, connectionManager: PeerConnectionManager, receiveIntervalMs: Long) {
        this.hyenaAddress = hyenaAddress
        this.connectionManager = connectionManager
        this.connection = connectionManager.getPeerConnection()
        scheduleKeepAliveThread()
        scheduleDataReceiverThread(receiveIntervalMs)
    }

    constructor(hyenaAddress: String, receiveIntervalMs: Long) : this(hyenaAddress, PeerConnectionManager(hyenaAddress), receiveIntervalMs)

    private fun scheduleDataReceiverThread(receiveIntervalMs: Long) {
        log.info("Scheduling receiving messages for ${this.connection.socketAddress} with $receiveIntervalMs ms interval")
        receiveScheduler.schedule(
                RECEIVE_START_DELAY_MS,
                receiveIntervalMs,
                {
                    try {
                        receiveData()
                    } catch (exc: EAgainException) {
                        // It means no message was waiting in nanomsg and we'll try later
                    }
                }
        )
    }

    private fun scheduleKeepAliveThread() {
        keepAliveScheduler.schedule(
                KEEP_ALIVE_START_DELAY_MS,
                KEEP_ALIVE_INTERVAL_MS,
                { keepAlive() }
        )
    }

    internal fun keepAlive() {
        try {
            sendKeepAlive()
        } catch (exc: nanomsg.exceptions.IOException) {
            log.error("Timeout on ${connection.socketAddress} socket", exc)
        }
    }

    private fun reconnect() {
        connection.close()
        connection = connectionManager.getPeerConnection()
    }

    private fun retrySend(body: () -> Unit) {
        var retrySend = true
        while (retrySend) {
            try {
                body()
                retrySend = false
            } catch (e: nanomsg.exceptions.IOException) {
                if (e.errno == SEND_TIMEOUT_ERRNO) {
                    log.warn("Received timeout. Reconnecting")
                    reconnect()
                    retrySend = true
                } else {
                    // Wrap in regular IOException
                    throw IOException("Nanomsg error: " + Nanomsg.getError(), e)
                }
            }
        }
    }

    private fun sendKeepAlive() {
        val bytes = MessageBuilder.buildKeepAliveRequest()
        retrySend { connection.synchronizedReq(bytes) }
    }

    open fun sendRequest(message: ByteArray): Future<*> {
        val messageId = UUID.randomUUID().leastSignificantBits
        val future = CompletableFuture<Any>()
        requests[messageId] = future
        retrySend {
            connection.synchronizedReq(request = MessageBuilder.wrapRequestIntoPeerRequest(PeerRequestType.Request, messageId, message))
            true
        }
        return future
    }

    private fun receiveData() {
        //TODO: do get_bytes in loop until all messages will be taken from socket
        val replyBuf = connection.getSynRespBufferNoWait()
        val reply = MessageDecoder.decodePeerReply(replyBuf)
        when (reply) {
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

    /**
     * Shuts down all connections and scheduled tasks.
     */
    internal fun shutDown() {
        keepAliveScheduler.cancel()
        receiveScheduler.cancel()
        connection.close()
    }

    companion object {
        private val log = Logger.get(ConnectionManager::class.java)
        internal const val SEND_TIMEOUT_ERRNO: Int = 110
    }
}
