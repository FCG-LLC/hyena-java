package co.llective.hyena.api

import co.llective.hyena.api.MessageDecoder.toUnsignedShort
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.throws
import io.airlift.slice.Slices
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder

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

    describe("Decode ColumnValues") {
        it("Correctly deserializes empty ColumnValues") {
            val byteBuffer = ByteBuffer.allocate(12)
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
            byteBuffer.putInt(BlockType.U32Dense.ordinal)
            byteBuffer.putLong(0)
            byteBuffer.position(0)

            val result = MessageDecoder.decodeColumnValues(byteBuffer)
            assert.that(result.type, equalTo(BlockType.U32Dense))
            assert.that(result.elementsCount, equalTo(0))
        }

        it("Correctly deserializes non-empty dense ColumnValues") {
            val value = Long.MAX_VALUE / 4
            val byteBuffer = ByteBuffer.allocate(20)
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
            byteBuffer.putInt(BlockType.I64Dense.ordinal)
            byteBuffer.putLong(1)
            byteBuffer.putLong(value)
            byteBuffer.position(0)

            val result = MessageDecoder.decodeColumnValues(byteBuffer)
            assert.that(result.type, equalTo(BlockType.I64Dense))
            assert.that(result.elementsCount, equalTo(1))
            assert.that(result.getLong(0), equalTo(value))
        }

        it("Correctly deserializes empty sparse ColumnValues") {
            val byteBuffer = ByteBuffer.allocate(20)
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
            byteBuffer.putInt(BlockType.U32Sparse.ordinal)
            byteBuffer.putLong(0)
            byteBuffer.putLong(0)
            byteBuffer.position(0)

            val result = MessageDecoder.decodeColumnValues(byteBuffer)
            assert.that(result.type, equalTo(BlockType.U32Sparse))
            assert.that(result.elementsCount, equalTo(0))
        }

        it("Correctly deserializes non-empty sparse ColumnValues") {
            val index = 15
            val value = 2
            val byteBuffer = ByteBuffer.allocate(26)
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
            byteBuffer.putInt(BlockType.U8Sparse.ordinal)
            // values
            byteBuffer.putLong(1)
            byteBuffer.put(value.toByte())
            // indexes
            byteBuffer.putLong(1)
            byteBuffer.putInt(index)

            byteBuffer.position(0)

            val result = MessageDecoder.decodeColumnValues(byteBuffer)
            assert.that(result.type, equalTo(BlockType.U8Sparse))
            assert.that(result.elementsCount, equalTo(1))
            assert.that(result.getLong(index), equalTo(value.toLong()))

            assert.that(result.isNull(0), equalTo(true))
            assert.that(result.isNull(index), equalTo(false))
        }

//        it("Correctly deserializes non-empty DenseStringColumn") {
//            val value1 = "one"
//            val value2 = "five"
//            val byteBuffer = ByteBuffer.allocate(69)
//            byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
//            byteBuffer.putInt(BlockType.StringDense.ordinal)
//            byteBuffer.putLong(2)   // records count
//            //metadata
//            byteBuffer.putLong(2) // metadata len
//            byteBuffer.putLong(0) // first string start index
//            byteBuffer.putLong(3) // first string len
//            byteBuffer.putLong(3) // second string start index
//            byteBuffer.putLong(4) // second string len
//            //string blob
//            byteBuffer.putLong(7) // blob len
//            byteBuffer.put(value1.toByteArray(HyenaApi.UTF8_CHARSET))
//            byteBuffer.put(value2.toByteArray(HyenaApi.UTF8_CHARSET))
//
//            byteBuffer.position(0)
//
//            val result = MessageDecoder.decodeColumnValues(byteBuffer)
//            assert.that(result.type, equalTo(BlockType.StringDense))
//            assert.that(result.elementsCount, equalTo(2))
//            assert.that(result.getSlice(0), equalTo(Slices.utf8Slice(value1)))
//            assert.that(result.getSlice(1), equalTo(Slices.utf8Slice(value2)))
//        }
        it("Correctly deserializes non-empty SimpleDenseStringColumn") {
            val value1 = "one"
            val value2 = "five"
            val byteBuffer = ByteBuffer.allocate(43)
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
            byteBuffer.putInt(BlockType.StringDense.ordinal)
            byteBuffer.putLong(2)   // records count
            //strings collection
            byteBuffer.putLong(2)   // records count (collection len)
            byteBuffer.putLong(value1.length.toLong()) // string1 len
            byteBuffer.put(value1.toByteArray(HyenaApi.UTF8_CHARSET))
            byteBuffer.putLong(value2.length.toLong()) // string2 len
            byteBuffer.put(value2.toByteArray(HyenaApi.UTF8_CHARSET))

            byteBuffer.position(0)

            val result = MessageDecoder.decodeColumnValues(byteBuffer)
            assert.that(result.type, equalTo(BlockType.StringDense))
            assert.that(result.elementsCount, equalTo(2))
            assert.that(result.getSlice(0), equalTo(Slices.utf8Slice(value1)))
            assert.that(result.getSlice(1), equalTo(Slices.utf8Slice(value2)))
        }
    }

    describe("Byte to unsigned short") {
        it("Converts correctly negative bytes to positive shorts") {
            var number: Byte = Byte.MIN_VALUE
            // generates sequence of bytes -128..-1
            val sequence = generateSequence {
                (number++).takeIf { it < 0 }
            }

            var short: Short = Byte.MAX_VALUE.toShort()
            for (byte: Byte in sequence) {
                // compares that sequence to corresponding 128..255 values
                assert.that(byte.toUnsignedShort(), equalTo(++short))
            }
        }

        it("Leaves positive bytes positive") {
            var number: Byte = Byte.MAX_VALUE
            // generates sequence of bytes 127..1
            val sequence = generateSequence {
                (number--).takeIf { it > 0 }
            }
            for (byte: Byte in sequence) {
                assert.that(byte.toUnsignedShort(), equalTo(byte.toShort()))
            }
        }

        it("Leaves 0 as it is") {
            val zeroByte: Byte = 0
            val zeroShort: Short = 0
            assert.that(zeroByte.toUnsignedShort(), equalTo(zeroShort))
        }
    }
})