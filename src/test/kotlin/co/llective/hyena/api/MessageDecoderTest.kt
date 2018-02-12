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
                    { MessageDecoder.decodeMessageType(byteBuffer) },
                    throws<DeserializationException>()
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

    describe("Decode BlockHolder") {
        it("Correctly deserializes empty DenseBlock") {
            val byteBuffer = ByteBuffer.allocate(12)
            byteBuffer.putInt(BlockType.U32Dense.ordinal)
            byteBuffer.putLong(0)
            byteBuffer.position(0)

            val result = MessageDecoder.decodeBlockHolder(byteBuffer)
            assert.that(result.type, equalTo(BlockType.U32Dense))
            assert.that(result.block.type, equalTo(BlockType.U32Dense))
            assert.that(result.block.count(), equalTo(0))
        }

        it("Correctly deserializes non-empty DenseBlock") {
            val value = Long.MAX_VALUE / 4
            val byteBuffer = ByteBuffer.allocate(20)
            byteBuffer.putInt(BlockType.I64Dense.ordinal)
            byteBuffer.putLong(1)
            byteBuffer.putLong(value)
            byteBuffer.position(0)

            val result = MessageDecoder.decodeBlockHolder(byteBuffer)
            assert.that(result.type, equalTo(BlockType.I64Dense))
            assert.that(result.block.type, equalTo(BlockType.I64Dense))
            assert.that(result.block.count(), equalTo(1))
            assert.that((result.block as DenseBlock<Long>).data.size, equalTo(1))
            assert.that((result.block as DenseBlock<Long>).data[0], equalTo(value))
        }

        it("Correctly deserializes empty SparseBlock") {
            val byteBuffer = ByteBuffer.allocate(20)
            byteBuffer.putInt(BlockType.U32Sparse.ordinal)
            byteBuffer.putLong(0)
            byteBuffer.putLong(0)
            byteBuffer.position(0)

            val result = MessageDecoder.decodeBlockHolder(byteBuffer)
            assert.that(result.type, equalTo(BlockType.U32Sparse))
            assert.that(result.block.type, equalTo(BlockType.U32Sparse))
            assert.that(result.block.count(), equalTo(0))
        }

        it("Correctly deserializes non-empty SparseBlock") {
            val index = 15
            val value = 2
            val byteBuffer = ByteBuffer.allocate(26)
            byteBuffer.putInt(BlockType.U8Sparse.ordinal)
            // values
            byteBuffer.putLong(1)
            byteBuffer.put(value.toByte())
            // indexes
            byteBuffer.putLong(1)
            byteBuffer.putInt(index)

            byteBuffer.position(0)

            val result = MessageDecoder.decodeBlockHolder(byteBuffer)
            assert.that(result.type, equalTo(BlockType.U8Sparse))
            assert.that(result.block.type, equalTo(BlockType.U8Sparse))
            assert.that(result.block.count(), equalTo(1))
            assert.that((result.block as SparseBlock<Short>).offsetData.size, equalTo(1))
            assert.that((result.block as SparseBlock<Short>).offsetData[0], equalTo(index))
            assert.that((result.block as SparseBlock<Short>).valueData.size, equalTo(1))
            assert.that((result.block as SparseBlock<Short>).valueData[0], equalTo(value.toShort()))
        }
    }
})