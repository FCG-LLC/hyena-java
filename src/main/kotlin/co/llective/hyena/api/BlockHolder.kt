package co.llective.hyena.api

import java.io.IOException
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.*

class BlockHolder {
    var type: BlockType? = null
    var block: Optional<Block> = Optional.empty()

    override fun toString(): String {
        val count = if (block.isPresent) { block.get().count } else 0
        return String.format("%s with %d elements", type!!.name, count)
    }

    companion object {

        @Throws(IOException::class)
        fun decode(buf: ByteBuffer): BlockHolder {
            val holder = BlockHolder()
            holder.type = BlockType.values()[buf.int]
            val recordsCount = buf.long.toInt()

            when (holder.type) {
                BlockType.String -> holder.block = Optional.of(StringBlock(recordsCount))
                BlockType.I8Dense  -> holder.block = Optional.of(DenseBlock<Byte>(holder.type!!, recordsCount))
                BlockType.I16Dense -> holder.block = Optional.of(DenseBlock<Short>(holder.type!!, recordsCount))
                BlockType.I32Dense -> holder.block = Optional.of(DenseBlock<Int>(holder.type!!, recordsCount))
                BlockType.I64Dense -> holder.block = Optional.of(DenseBlock<Long>(holder.type!!, recordsCount))
                BlockType.U8Dense  -> holder.block = Optional.of(DenseBlock<Short>(holder.type!!, recordsCount))
                BlockType.U16Dense -> holder.block = Optional.of(DenseBlock<Int>(holder.type!!, recordsCount))
                BlockType.U32Dense -> holder.block = Optional.of(DenseBlock<Long>(holder.type!!, recordsCount))
                BlockType.U64Dense -> holder.block = Optional.of(DenseBlock<BigInteger>(holder.type!!, recordsCount))
                BlockType.I8Sparse  -> holder.block = Optional.of(SparseBlock<Byte>(holder.type!!, recordsCount))
                BlockType.I16Sparse -> holder.block = Optional.of(SparseBlock<Short>(holder.type!!, recordsCount))
                BlockType.I32Sparse -> holder.block = Optional.of(SparseBlock<Int>(holder.type!!, recordsCount))
                BlockType.I64Sparse -> holder.block = Optional.of(SparseBlock<Long>(holder.type!!, recordsCount))
                BlockType.U8Sparse  -> holder.block = Optional.of(SparseBlock<Short>(holder.type!!, recordsCount))
                BlockType.U16Sparse -> holder.block = Optional.of(SparseBlock<Int>(holder.type!!, recordsCount))
                BlockType.U32Sparse -> holder.block = Optional.of(SparseBlock<Long>(holder.type!!, recordsCount))
                BlockType.U64Sparse -> holder.block = Optional.of(SparseBlock<BigInteger>(holder.type!!, recordsCount))
            }

            for (i in 0 until recordsCount) {
                when (holder.type) {
                    BlockType.String -> (holder.block.get() as StringBlock).add(buf.int, buf.long)
                    BlockType.I8Dense -> (holder.block.get() as DenseBlock<*>).add(buf.get())
                    BlockType.I16Dense -> (holder.block.get() as DenseBlock<*>).add(buf.short)
                    BlockType.I32Dense -> (holder.block.get() as DenseBlock<*>).add(buf.int)
                    BlockType.I64Dense -> (holder.block.get() as DenseBlock<*>).add(buf.long)
                    BlockType.U8Dense -> TODO()
                    BlockType.U16Dense -> TODO()
                    BlockType.U32Dense -> TODO()
                    BlockType.U64Dense -> TODO()
                    BlockType.I8Sparse -> (holder.block.get() as SparseBlock<*>).add(buf.int, buf.get())
                    BlockType.I16Sparse -> (holder.block.get() as SparseBlock<*>).add(buf.int, buf.short)
                    BlockType.I32Sparse -> (holder.block.get() as SparseBlock<*>).add(buf.int, buf.int)
                    BlockType.I64Sparse -> (holder.block.get() as SparseBlock<*>).add(buf.int, buf.long)
                    BlockType.U8Sparse -> TODO()
                    BlockType.U16Sparse -> TODO()
                    BlockType.U32Sparse -> TODO()
                    BlockType.U64Sparse -> TODO()
                    null -> TODO()
                }
            }

            if (holder.type == BlockType.String) {
                val len = buf.long.toInt()
                val block = holder.block.get() as StringBlock
                block.bytes = ByteArray(len)
                buf.get(block.bytes, 0, len)
            }

            return holder
        }
    }
}