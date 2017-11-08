package co.llective.hyena.api

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.math.BigInteger

object MessageDecoderTest : Spek({
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