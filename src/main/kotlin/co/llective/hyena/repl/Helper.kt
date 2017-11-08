package co.llective.hyena.repl

import co.llective.hyena.api.*
import java.math.BigInteger
import java.util.Random
import java.util.ArrayList

/** This object contains a set of utility function to help play with the REPL */
object Helper {
    @JvmStatic fun randomTimestamps(n: Int) : List<Long> {
        val list = ArrayList<Long>(n)
        val generator = Random()
        (0 until n).map { list.add(Math.abs(generator.nextLong())) }
        list.sort()

        return list
    }

    @JvmStatic fun randomDenseBlock(n: Int, type: BlockType) : Block {
        val block: DenseBlock<*> = createBlock(n, type) as DenseBlock<*>
        val generator = Random()
        (0 until n).forEach {
            block.add(when (type) {
                BlockType.I8Dense -> (generator.nextInt(256) - 128).toByte()
                BlockType.I16Dense -> (generator.nextInt(65536) - 32768).toShort()
                BlockType.I32Dense -> generator.nextInt()
                BlockType.I64Dense -> generator.nextLong()
                BlockType.U8Dense -> generator.nextInt(256)
                BlockType.U16Dense -> generator.nextInt(65536)
                BlockType.U32Dense -> generator.nextInt(Int.MAX_VALUE)
                BlockType.U64Dense -> Math.abs(generator.nextLong())
                else ->
                    throw IllegalArgumentException("randomDenseColumn cannot be used to build sparse or String column")
            })
        }

        return block
    }

    @JvmStatic
    fun randomSparseBlock(n: Int, type: BlockType) : Block {
        val block: SparseBlock<*> = createBlock(n, type) as SparseBlock<*>
        val generator = Random()
        val offsets = (0 until n).map{ generator.nextInt(Int.MAX_VALUE) }
        offsets.sorted().forEach { offset ->
            block.add(
                    offset,
                    when (type) {
                        BlockType.I8Sparse -> (generator.nextInt(256) - 128).toByte()
                        BlockType.I16Sparse -> (generator.nextInt(65536) - 32768).toShort()
                        BlockType.I32Sparse -> generator.nextInt()
                        BlockType.I64Sparse -> generator.nextLong()
                        BlockType.U8Sparse -> generator.nextInt(256)
                        BlockType.U16Sparse -> generator.nextInt(65536)
                        BlockType.U32Sparse -> generator.nextInt(Int.MAX_VALUE)
                        BlockType.U64Sparse -> Math.abs(generator.nextLong())
                        else ->
                            throw IllegalArgumentException("randomSparseColumn cannot be used to build dense or String column")
                    })
        }

        return block
    }

    private fun createBlock(n: Int, type: BlockType) : Block {
        return when(type) {
            BlockType.I8Dense  -> DenseBlock<Byte>(type, n)
            BlockType.I16Dense -> DenseBlock<Short>(type, n)
            BlockType.I32Dense -> DenseBlock<Int>(type, n)
            BlockType.I64Dense -> DenseBlock<Long>(type, n)
            BlockType.U8Dense  -> DenseBlock<Short>(type, n)
            BlockType.U16Dense -> DenseBlock<Int>(type, n)
            BlockType.U32Dense -> DenseBlock<Long>(type, n)
            BlockType.U64Dense -> DenseBlock<BigInteger>(type, n)
            BlockType.I8Sparse  -> SparseBlock<Byte>(type, n)
            BlockType.I16Sparse -> SparseBlock<Short>(type, n)
            BlockType.I32Sparse -> SparseBlock<Int>(type, n)
            BlockType.I64Sparse -> SparseBlock<Long>(type, n)
            BlockType.U8Sparse  -> SparseBlock<Short>(type, n)
            BlockType.U16Sparse -> SparseBlock<Int>(type, n)
            BlockType.U32Sparse -> SparseBlock<Long>(type, n)
            BlockType.U64Sparse -> SparseBlock<BigInteger>(type, n)
            BlockType.String -> StringBlock(n)
        }
    }
}