package co.llective.hyena.api

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.sameInstance
import com.natpryce.hamkrest.throws
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doNothing
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.util.*

object HyenaApiTest : Spek({
    describe("List columns") {
        it("Throws on wrong reply") {
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn CatalogReply(mock())
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            assert.that({sut.listColumns()}, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val list = mock<List<Column>>()
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn ListColumnsReply(list)
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val reply = sut.listColumns()
            assert.that(list, sameInstance(reply))
        }
    }

    describe("Add column") {
        it("Throws on wrong reply") {
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn CatalogReply(mock())
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val column = Column(BlockType.I32Dense, 100, "testColumn")
            assert.that({sut.addColumn(column)}, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn AddColumnReply(Left(10))
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val column = Column(BlockType.I32Dense, 100, "testColumn")
            val result = sut.addColumn(column)
            assert.that(true, equalTo(result.isPresent))
            assert.that(10, equalTo(result.get()))
        }

        it("Correctly extracts the error") {
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn
                        AddColumnReply(Right(ApiError(ApiErrorType.Unknown, Optional.empty())))
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val column = Column(BlockType.I32Dense, 100, "testColumn")
            val result = sut.addColumn(column)
            assert.that(false, equalTo(result.isPresent))
        }
    }

    describe("Insert") {
        it("Throws on wrong reply") {
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn CatalogReply(mock())
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val data = ColumnData(100, DenseBlock<Int>(BlockType.I32Dense, 10))
            assert.that({sut.insert(10, listOf(), data)}, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn InsertReply(Left(100))
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val data = ColumnData(100, DenseBlock<Int>(BlockType.I32Dense, 10))
            val reply = sut.insert(10, listOf(), data)
            assert.that(true, equalTo(reply.isPresent))
            assert.that(100, equalTo(reply.get()))
        }

        it("Correctly extracts the error") {
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn
                        InsertReply(Right(ApiError(ApiErrorType.Unknown, Optional.empty())))
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val data = ColumnData(100, DenseBlock<Int>(BlockType.I32Dense, 10))
            val reply = sut.insert(10, listOf(), data)
            assert.that(false, equalTo(reply.isPresent))
        }
    }

    describe("Refresh catalog") {
        it("Throws on wrong reply") {
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn ListColumnsReply(mock())
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            assert.that({sut.refreshCatalog()}, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val catalog = mock<Catalog>()
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn CatalogReply(catalog)
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val reply = sut.refreshCatalog()
            assert.that(catalog, sameInstance(reply))
        }
    }

    describe("Scan") {
        it("Throws on wrong reply") {
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn CatalogReply(mock())
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val req = ScanRequest(0, 10, 100, listOf(), listOf())
            assert.that({sut.scan(req, null)}, throws<ReplyException>())
        }

        it("Correctly extracts the reply") {
            val scanResult = mock<ScanResult>()
            val connection = mock<HyenaConnection> {
                on { sendAndReceive(any()) } doReturn ScanReply(scanResult)
            }
            doNothing().`when`(connection).ensureConnected()
            val sut = HyenaApi(connection)

            val req = ScanRequest(0, 10, 100, listOf(), listOf())
            val reply = sut.scan(req, null)
            assert.that(scanResult, sameInstance(reply))
        }
    }
})