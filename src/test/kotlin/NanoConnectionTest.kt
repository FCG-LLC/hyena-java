
import co.llective.hyena.NanoConnection
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.nhaarman.mockito_kotlin.*
import nanomsg.Socket
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object NanoConnectionTest: Spek({
    describe("EnsureConnection") {
        it("creates and connects to new socket instance if not connected") {
            val mockedSocket = mock<Socket>()

            open class MockedConnection(socketAddress: String) : NanoConnection(socketAddress) {
                override var socket: Socket = mock()
                override fun newSocketInstance(): Socket {
                    return mockedSocket
                }
            }

            val connection = spy(MockedConnection(""))

            assert.that(false, equalTo(connection.connected))

            connection.ensureConnection()

            verify(connection).newSocketInstance()
            verify(mockedSocket).connect(any())
            assert.that(connection.connected, equalTo(true))
        }

        it("does nothing when connected") {
            val mockedSocket = mock<Socket>()

            open class MockedConnection(socketAddress: String) : NanoConnection(socketAddress) {
                override var socket: Socket = mock()
                override fun newSocketInstance(): Socket {
                    return mockedSocket
                }
            }

            val connection = spy(MockedConnection(""))

            connection.connected = true

            connection.ensureConnection()

            verify(connection, never()).newSocketInstance()
            verify(mockedSocket, never()).connect(any())
            assert.that(connection.connected, equalTo(true))
        }
    }

    describe("Close") {
        it("closes when connected") {
            val mockedSocket = mock<Socket>()

            open class MockedConnection(socketAddress: String) : NanoConnection(socketAddress) {
                override var socket: Socket = mockedSocket
                override fun newSocketInstance(): Socket {
                    return mock()
                }
            }

            val connection = spy(MockedConnection(""))
            connection.connected = true

            connection.close()

            verify(mockedSocket).close()
            assert.that(connection.connected, equalTo(false))
        }

        it("does nothing when not connected") {
            val mockedSocket = mock<Socket>()

            open class MockedConnection(socketAddress: String) : NanoConnection(socketAddress) {
                override var socket: Socket = mockedSocket
                override fun newSocketInstance(): Socket {
                    return mock()
                }
            }

            val connection = spy(MockedConnection(""))
            connection.connected = false

            connection.close()

            verify(mockedSocket, never()).close()
            assert.that(connection.connected, equalTo(false))
        }
    }
})