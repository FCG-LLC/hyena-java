package co.llective.hyena.api

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.throws
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.math.BigInteger
import java.nio.ByteBuffer

object MessageDecoderTest : Spek({
    describe("Decode Message Type") {
        it("Correctly decodes Message Types") {
            for ((messageTypeId, messageType) in ApiRequest.values().withIndex()) {
                val byteBuffer = ByteBuffer.allocate(16)
                byteBuffer.putInt(messageTypeId)
                byteBuffer.putInt(123)      //rest of the message
                byteBuffer.position(0) //reset position

                val actualMessageType = MessageDecoder.decodeMessageType(byteBuffer)
                assert.that(actualMessageType, equalTo(messageType))
            }
        }

        it("Throws on wrong message type id") {
            val tooBigId = ApiRequest.values().size + 1
            val byteBuffer = ByteBuffer.allocate(16)
            byteBuffer.putInt(tooBigId)
            byteBuffer.putInt(123)      //rest of the message
            byteBuffer.position(0) //reset position

            assert.that(
                {MessageDecoder.decodeMessageType(byteBuffer)},
                throws<MessageDecoder.DeserializationException>()
            )
        }
    }

    describe("Decode BigInt") {
        it("Correctly decodes positive numbers") {
            val bi = MessageDecoder.decodeBigInt(0)
            assert.that(bi, equalTo(BigInteger.ZERO))

            val bi2 = MessageDecoder.decodeBigInt(100L)
            assert.that(bi2, equalTo(BigInteger.valueOf(100L)))

            val bi3 = MessageDecoder.decodeBigInt(Long.MAX_VALUE)
            assert.that(bi3, equalTo(BigInteger.valueOf(Long.MAX_VALUE)))
        }

        it("Correctly decodes negative numbers") {
            val bi1 = MessageDecoder.decodeBigInt(-1);
            assert.that(bi1, equalTo(BigInteger("18446744073709551615")))

            val bi2 = MessageDecoder.decodeBigInt(-2);
            assert.that(bi2, equalTo(BigInteger("18446744073709551614")))

            val bi3 = MessageDecoder.decodeBigInt(Long.MIN_VALUE);
            assert.that(bi3, equalTo(BigInteger("9223372036854775808")))
        }
    }
})