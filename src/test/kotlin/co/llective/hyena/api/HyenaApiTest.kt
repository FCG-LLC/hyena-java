package co.llective.hyena.api

import co.llective.hyena.PeerConnection
import co.llective.hyena.PeerConnectionManager
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.sameInstance
import com.natpryce.hamkrest.throws
import com.nhaarman.mockito_kotlin.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.Future

object HyenaApiTest : Spek({
    describe("List columns") {
        it("Throws on wrong reply") {
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn CatalogReply(mock())
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }

            val sut = HyenaApi(connectionManager)

            assert.that({ sut.listColumns() }, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val list = mock<List<Column>>()
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn ListColumnsReply(list)
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }

            val sut = HyenaApi(connectionManager)

            val reply = sut.listColumns()
            assert.that(list, sameInstance(reply))
        }
    }

    describe("Add column") {
        it("Throws on wrong reply") {
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn CatalogReply(mock())
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            val column = Column(BlockType.I32Dense, 100, "testColumn")
            assert.that({ sut.addColumn(column) }, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn AddColumnReply(Left(10))
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }

            val sut = HyenaApi(connectionManager)

            val column = Column(BlockType.I32Dense, 100, "testColumn")
            val result = sut.addColumn(column)
            assert.that(true, equalTo(result.isPresent))
            assert.that(10, equalTo(result.get()))
        }

        it("Correctly extracts the error") {
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn AddColumnReply(Right(ApiError(ApiErrorType.Unknown, Optional.empty())))
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            val column = Column(BlockType.I32Dense, 100, "testColumn")
            val result = sut.addColumn(column)
            assert.that(false, equalTo(result.isPresent))
        }
    }

    describe("Insert") {
        it("Throws on wrong reply") {
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn CatalogReply(mock())
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            val data = ColumnData(100, DenseBlock<Int>(BlockType.I32Dense, 10))
            assert.that({ sut.insert(10, listOf(), listOf(data)) }, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn InsertReply(Left(100))
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            val data = ColumnData(100, DenseBlock<Int>(BlockType.I32Dense, 10))
            val reply = sut.insert(10, listOf(), listOf(data))
            assert.that(true, equalTo(reply.isPresent))
            assert.that(100, equalTo(reply.get()))
        }

        it("Correctly extracts the error") {
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn InsertReply(Right(ApiError(ApiErrorType.Unknown, Optional.empty())))
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            val data = ColumnData(100, DenseBlock<Int>(BlockType.I32Dense, 10))
            val reply = sut.insert(10, listOf(), listOf(data))
            assert.that(false, equalTo(reply.isPresent))
        }
    }

    describe("Refresh catalog") {
        it("Throws on wrong reply") {
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn ListColumnsReply(mock())
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            assert.that({ sut.refreshCatalog() }, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val catalog = mock<Catalog>()
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn CatalogReply(catalog)
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            val reply = sut.refreshCatalog()
            assert.that(catalog, sameInstance(reply))
        }

        it("Refresh can be forced") {
            val catalog1 = mock<Catalog>()
            val catalog2 = mock<Catalog>()
            var called = false
            val mockedFuture = mock<Future<Any>> {
                on { get() } doAnswer {
                    if (!called) {
                        called = true
                        CatalogReply(catalog1)
                    } else {
                        CatalogReply(catalog2)
                    }
                }
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            var reply = sut.refreshCatalog()
            assert.that(catalog1, sameInstance(reply))

            reply = sut.refreshCatalog()
            assert.that(catalog1, sameInstance(reply))

            reply = sut.refreshCatalog(true)
            assert.that(catalog2, sameInstance(reply))
        }
    }

    describe("Scan") {
        it("Throws on wrong reply") {
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn CatalogReply(mock())
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            val partitionIds = HashSet<UUID>()
            partitionIds.add(UUID.randomUUID())

            val req = ScanRequest(0, 10, partitionIds, ScanOrFilters(), listOf())
            assert.that({ sut.scan(req) }, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val scanResult = ScanResult(mutableMapOf())
            val mockedFuture = mock<Future<Any>> {
                on { get() } doReturn ScanReply(Left(scanResult))
            }
            val connectionManager = mock<ConnectionManager> {
                on { sendRequest(any()) } doReturn mockedFuture
            }
            val sut = HyenaApi(connectionManager)

            val partitionIds = HashSet<UUID>()
            partitionIds.add(UUID.randomUUID())

            val req = ScanRequest(0, 10, partitionIds, ScanOrFilters(), listOf())
            val reply = sut.scan(req)
            assert.that(scanResult, sameInstance(reply))
        }
    }
})

object ConnectionManagerTest : Spek({

    val hyenaAddress = "hyenaAddress"

    describe("KeepAlive") {
        it("sends keep alive request when socket looks fine") {
            val serializedKeepAliveReq = MessageBuilder.buildKeepAliveRequest()
            val mockedConnection = mock<PeerConnection> {}
            doNothing().whenever(mockedConnection).synchronizedReq(serializedKeepAliveReq)
            doNothing().whenever(mockedConnection).close()
            val mockedManager = mock<PeerConnectionManager> {
                on { getPeerConnection() } doReturn mockedConnection
            }
            val connectionManager = ConnectionManager(hyenaAddress, mockedManager)

            connectionManager.keepAlive()
            verify(mockedConnection, atLeast(1)).synchronizedReq(serializedKeepAliveReq)

            connectionManager.shutDown()
        }

        it("resets connection when keep alive failed") {
            val serializedKeepAliveReq = MessageBuilder.buildKeepAliveRequest()
            val mockedConnection = mock<PeerConnection> {}

            whenever(mockedConnection.synchronizedReq(serializedKeepAliveReq))
                    .thenThrow(nanomsg.exceptions.IOException("Testing", 110))
                    .then { /* Do nothing */ }
            doNothing().whenever(mockedConnection).close()
            val mockedManager = mock<PeerConnectionManager> {
                on { getPeerConnection() } doReturn mockedConnection
            }
            val connectionManager = ConnectionManager(hyenaAddress, mockedManager)
            connectionManager.keepAliveResponse.set(false)

            connectionManager.keepAlive()
            verify(mockedConnection, atLeast(1)).close()
            verify(mockedManager, atLeast(1)).getPeerConnection()

            connectionManager.shutDown()
        }
    }
})