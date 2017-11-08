package co.llective.hyena.api

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.throws
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.io.DataOutput
import java.lang.IllegalArgumentException

object DenseBlockTest : Spek({
    describe("DenseBlock") {
        on("construction") {
            it("Cannot be created with Sparse data type") {
                assert.that(
                    { DenseBlock<Int>(BlockType.I32Sparse, 4) },
                    throws<IllegalArgumentException>())
            }

            it("Cannot be created with zero or negative size") {
                assert.that(
                    { DenseBlock<Int>(BlockType.I32Dense, 0) },
                    throws<IllegalArgumentException>())
                assert.that(
                    { DenseBlock<Int>(BlockType.I32Dense, -16) },
                    throws<IllegalArgumentException>())
            }
        }

        on("writing") {
            val block = DenseBlock<Int>(BlockType.I32Dense, 4)
                    .add(1).add(1).add(1).add(1)
            val dos = mock<DataOutput>()

            it("writes data size, then writes data") {
                block.write(dos)

                verify(dos, times(1)).writeLong(4)
                verify(dos, times(4)).writeInt(1)
            }
        }
    }
})

object SparseBlockTest : Spek({
    describe("SparseBlock") {
        on("construction") {
            it("Cannot be created with Dense data type") {
                assert.that(
                    { SparseBlock<Int>(BlockType.I32Dense, 4) },
                    throws<IllegalArgumentException>())
            }

            it("Cannot be created with zero or negative size") {
                assert.that(
                    { SparseBlock<Int>(BlockType.I32Sparse, 0) },
                    throws<IllegalArgumentException>())
                assert.that(
                    { SparseBlock<Int>(BlockType.I32Sparse, -16) },
                    throws<IllegalArgumentException>())
            }
        }

        on("writing") {
            val block = SparseBlock<Int>(BlockType.I32Sparse, 4)
                    .add(0, 1).add(5, 1).add(10, 1).add(15, 1)
            val dos = mock<DataOutput>()

            it("writes data, then writes indexes") {
                block.write(dos)

                verify(dos, times(2)).writeLong(4)
                verify(dos, times(4)).writeInt(1)
                verify(dos, times(1)).writeInt(0)
                verify(dos, times(1)).writeInt(5)
                verify(dos, times(1)).writeInt(10)
                verify(dos, times(1)).writeInt(15)
            }
        }
    }
})

object StringBlockTest : Spek({
    on("construction") {
        it("Cannot be created with zero or negative size") {
            assert.that({ StringBlock(0) }, throws<IllegalArgumentException>())
            assert.that({ StringBlock(-16) }, throws<IllegalArgumentException>())
        }
    }
})