package co.llective.hyena

import co.llective.hyena.api.MessageBuilder
import co.llective.hyena.api.MessageDecoder
import io.airlift.log.Logger
import nanomsg.Nanomsg
import nanomsg.Socket
import nanomsg.pair.PairSocket
import nanomsg.reqrep.ReqSocket
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

/**
 * Thread-safe (synchronized) abstract nanomsg connection.
 */
abstract class NanoConnection(val socketAddress: String, internal var connected: Boolean = false) {

    internal val lock = Object()
    internal abstract var socket: Socket

    abstract fun newSocketInstance(): Socket

    internal open fun ensureConnection() {
        synchronized(lock) {
            if (!connected) {
                socket = newSocketInstance()
                log.info("Opening new connection to: $socketAddress")
                socket.setSocketOpt(Nanomsg.SocketOption.NN_RCVTIMEO, 60000)
                socket.setSocketOpt(Nanomsg.SocketOption.NN_SNDTIMEO, 60000)
                socket.connect(socketAddress)
                connected = true
            }
        }
    }

    /**
     * Not thread-safe operation!
     */
    internal fun getRespBuffer(wait: Boolean): ByteBuffer {
        ensureConnection()
        val replyBytes: ByteArray =
                if (wait)
                    socket.recvBytes()
                else
                    socket.recvBytes(EnumSet.of(Nanomsg.SocketFlag.NN_DONTWAIT))
        val replyBuf = ByteBuffer.wrap(replyBytes)
        replyBuf.order(ByteOrder.LITTLE_ENDIAN)
        return replyBuf
    }

    open fun synchronizedReq(request: ByteArray) {
        ensureConnection()

        synchronized(lock) {
            socket.send(request)
        }
    }

    /**
     * Closes socket connection. Invalidates socket.
     */
    open fun close() {
        //TODO: send abort message
        synchronized(lock) {
            if (connected) {
                connected = false
                socket.close()
            }
        }
    }

    companion object {
        internal val log = Logger.get(NanoConnection::class.java)
    }
}

/**
 * PairSocket over Nanomsg.
 */
open class PeerConnection(val connectionId: Long, socketAddress: String) : NanoConnection(socketAddress) {
    override lateinit var socket: Socket

    override fun newSocketInstance(): Socket = PairSocket()

    fun getSynRespBufferNoWait(): ByteBuffer {
        synchronized(lock) {
            return getRespBuffer(false)
        }
    }
}


/**
 * Connection for issuing further connections to Hyena.
 * ReqRepSocket over Nanomsg.
 */
open class PeerConnectionManager(socketAddress: String) : NanoConnection(socketAddress) {
    override lateinit var socket: Socket

    override fun newSocketInstance(): Socket = ReqSocket()

    /**
     * Performs synchronized request and awaits response.
     * @param request ByteArray written with LittleEndianess
     * @return ByteBuffer in Little Endian order
     */
    internal fun synchronizedReqResp(request: ByteArray): ByteBuffer? {
        ensureConnection()

        try {
            var replyBuf: ByteBuffer? = null
            synchronized(lock) {
                socket.send(request)
                replyBuf = getRespBufferWait()
            }
            return replyBuf
        } catch (exc: IOException) {
            throw IOException("Nanomsg error: " + Nanomsg.getError(), exc)
        }
    }

    private fun getRespBufferWait(): ByteBuffer {
        return getRespBuffer(true)
    }

    /**
     * Issues PeerConnection to hyena.
     * @return Initialized split connection to hyena.
     */
    internal open fun getPeerConnection(): PeerConnection {
        val issueConnectionMessage = MessageBuilder.buildConnectMessage()
        val responseBuffer = synchronizedReqResp(issueConnectionMessage)
        val connectionResponse = MessageDecoder.decodeControlReply(responseBuffer!!)
        close()
        return PeerConnection(connectionResponse.connectionId, connectionResponse.socketAddress)
    }
}