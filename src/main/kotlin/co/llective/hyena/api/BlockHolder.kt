package co.llective.hyena.api

import java.io.IOException
import java.nio.ByteBuffer

class BlockHolder {
    var type: BlockType? = null
    var int64DenseBlock: DenseBlock<Long>? = null
    var int64SparseBlock: SparseBlock<Long>? = null
    var int32SparseBlock: SparseBlock<Int>? = null
    var int16SparseBlock: SparseBlock<Short>? = null
    var int8SparseBlock: SparseBlock<Byte>? = null
    var stringBlock: StringBlock? = null

    override fun toString(): String {
        var count = 0
        when (type) {
            BlockType.Int8Sparse -> count = int8SparseBlock!!.offsetData.size
            BlockType.Int16Sparse -> count = int16SparseBlock!!.offsetData.size
            BlockType.Int32Sparse -> count = int32SparseBlock!!.offsetData.size
            BlockType.Int64Sparse -> count = int64SparseBlock!!.offsetData.size
            BlockType.Int64Dense -> count = int64DenseBlock!!.data.size
            BlockType.String -> count = stringBlock!!.offsetData.size
        }

        return String.format("%s with %d elements", type!!.name, count)
    }

    companion object {

        @Throws(IOException::class)
        fun decode(buf: ByteBuffer): BlockHolder {
            val holder = BlockHolder()
            holder.type = BlockType.values()[buf.int]
            val recordsCount = buf.long.toInt()

            when (holder.type) {
                BlockType.Int8Sparse -> holder.int8SparseBlock = SparseBlock(recordsCount)
                BlockType.Int16Sparse -> holder.int16SparseBlock = SparseBlock(recordsCount)
                BlockType.Int32Sparse -> holder.int32SparseBlock = SparseBlock(recordsCount)
                BlockType.Int64Sparse -> holder.int64SparseBlock = SparseBlock(recordsCount)
                BlockType.Int64Dense -> holder.int64DenseBlock = DenseBlock(recordsCount)
                BlockType.String -> holder.stringBlock = StringBlock(recordsCount)
            }

            for (i in 0 until recordsCount) {
                when (holder.type) {
                    BlockType.Int8Sparse -> {
                        holder.int8SparseBlock!!.offsetData.add(buf.int)
                        holder.int8SparseBlock!!.valueData.add(buf.get())
                    }
                    BlockType.Int16Sparse -> {
                        holder.int16SparseBlock!!.offsetData.add(buf.int)
                        holder.int16SparseBlock!!.valueData.add(buf.short)
                    }
                    BlockType.Int32Sparse -> {
                        holder.int32SparseBlock!!.offsetData.add(buf.int)
                        holder.int32SparseBlock!!.valueData.add(buf.int)
                    }
                    BlockType.Int64Sparse -> {
                        holder.int64SparseBlock!!.offsetData.add(buf.int)
                        holder.int64SparseBlock!!.valueData.add(buf.long)
                    }
                    BlockType.Int64Dense -> holder.int64DenseBlock!!.data.add(buf.long)
                    BlockType.String -> {
                        holder.stringBlock!!.offsetData.add(buf.int)
                        holder.stringBlock!!.valueStartPositions.add(buf.long)
                    }
                }
            }

            if (holder.type == BlockType.String) {
                val len = buf.long.toInt()
                holder.stringBlock!!.bytes = ByteArray(len)
                buf.get(holder.stringBlock!!.bytes!!, 0, len)
            }

            return holder
        }
    }
}