package co.llective.hyena.repl

import co.llective.hyena.api.*
import java.math.BigInteger
import java.util.*

/** This object contains a set of utility function to help play with the REPL */
object Helper {
    @JvmStatic
    fun randomTimestamps(n: Int): List<Long> {
        val list = ArrayList<Long>(n)
        val generator = Random()
        (0 until n).map { list.add(Math.abs(generator.nextLong())) }
        list.sort()

        return list
    }

    @JvmStatic
    fun randomDenseBlock(n: Int, type: BlockType): Block {
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
    fun randomSparseBlock(n: Int, type: BlockType): SparseBlock<*> {
        val sparseBlock = when (type) {
            BlockType.I8Sparse -> SparseBlock<Byte>(type, n)
            BlockType.I16Sparse -> SparseBlock<Short>(type, n)
            BlockType.I32Sparse -> SparseBlock<Int>(type, n)
            BlockType.I64Sparse -> SparseBlock<Long>(type, n)
            BlockType.U8Sparse -> SparseBlock<Short>(type, n)
            BlockType.U16Sparse -> SparseBlock<Int>(type, n)
            BlockType.U32Sparse -> SparseBlock<Long>(type, n)
            BlockType.U64Sparse -> SparseBlock<BigInteger>(type, n)
            else ->
                throw IllegalArgumentException("cannot create random block for $type block type")
        }

        return fillRandomSparseBlock(sparseBlock, n)
    }

    private fun <T> fillRandomSparseBlock(sparseBlock: SparseBlock<T>, n: Int): SparseBlock<T> {
        val generator = Random()
        val offsets = (0 until n).map { generator.nextInt(Int.MAX_VALUE) }
        val valueList: MutableList<T> = ArrayList(n)
        (0 until n).forEach {
            when (sparseBlock.type) {
                BlockType.I8Sparse -> valueList.add((generator.nextInt(256) - 128).toByte() as T)
                BlockType.I16Sparse -> valueList.add((generator.nextInt(65536) - 32768).toShort() as T)
                BlockType.I32Sparse -> valueList.add(generator.nextInt() as T)
                BlockType.I64Sparse -> valueList.add(generator.nextLong() as T)
                BlockType.U8Sparse -> valueList.add(generator.nextInt(256) as T)
                BlockType.U16Sparse -> valueList.add(generator.nextInt(65536) as T)
                BlockType.U32Sparse -> valueList.add(generator.nextInt(Int.MAX_VALUE) as T)
                BlockType.U64Sparse -> valueList.add(Math.abs(generator.nextLong()) as T)
                else ->
                    throw IllegalArgumentException("randomSparseColumn cannot be created for " + sparseBlock.type + " type")
            }
        }

        offsets.zip(valueList).forEach { (index, value) -> sparseBlock.add(index, value) }

        return sparseBlock
    }

    @JvmStatic
    fun convertValue(value: String, type: FilterType): Any =
        when (type) {
            FilterType.I8   -> Integer.parseInt(value)
            FilterType.I16  -> Integer.parseInt(value)
            FilterType.I32  -> Integer.parseInt(value)
            FilterType.I64  -> java.lang.Long.parseLong(value)
            FilterType.I128 -> TODO()
            FilterType.U8   -> Integer.parseInt(value)
            FilterType.U16  -> Integer.parseInt(value)
            FilterType.U32  -> java.lang.Long.parseLong(value)
            //FilterType.U64  -> MessageDecoder.decodeBigInt(java.lang.Long.parseLong(value))
            FilterType.U64  -> java.lang.Long.parseLong(value)
            FilterType.U128 -> TODO()
            FilterType.String -> value
        }


    private fun createBlock(n: Int, type: BlockType): Block {
        return when (type) {
            BlockType.I8Dense -> DenseBlock<Byte>(type, n)
            BlockType.I16Dense -> DenseBlock<Short>(type, n)
            BlockType.I32Dense -> DenseBlock<Int>(type, n)
            BlockType.I64Dense -> DenseBlock<Long>(type, n)
            BlockType.U8Dense -> DenseBlock<Short>(type, n)
            BlockType.U16Dense -> DenseBlock<Int>(type, n)
            BlockType.U32Dense -> DenseBlock<Long>(type, n)
            BlockType.U64Dense -> DenseBlock<BigInteger>(type, n)
            BlockType.I8Sparse -> SparseBlock<Byte>(type, n)
            BlockType.I16Sparse -> SparseBlock<Short>(type, n)
            BlockType.I32Sparse -> SparseBlock<Int>(type, n)
            BlockType.I64Sparse -> SparseBlock<Long>(type, n)
            BlockType.U8Sparse -> SparseBlock<Short>(type, n)
            BlockType.U16Sparse -> SparseBlock<Int>(type, n)
            BlockType.U32Sparse -> SparseBlock<Long>(type, n)
            BlockType.U64Sparse -> SparseBlock<BigInteger>(type, n)
            BlockType.String -> StringBlock(n)
            else -> {
                // 128bit
                TODO("implement")
            }
        }
    }
}