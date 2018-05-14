package co.llective.hyena.api

import io.airlift.slice.Slice

/**
 * Contains data for given column.
 */
abstract class ColumnValues {
    abstract val type: BlockType
    abstract val elementsCount: Int
    abstract val elementBytesSize: Int
    protected abstract val dataSlice: Slice

    /**
     * Gets long value for given row id.
     * If doesn't exist there can be wrong output.
     * Use [isNull] before to make sure.
     */
    abstract fun getLong(rowId: Int): Long

    /**
     * Gets value for given row id in form of byte array.
     * If doesn't exist there can be wrong output.
     * Use [isNull] before to make sure.
     */
    abstract fun getBytes(rowId: Int): ByteArray

    /**
     * Returns info if value of given row id exist.
     */
    abstract fun isNull(rowId: Int): Boolean

    /**
     * Allows to ask for data from rows before.
     *
     * Note: Only valid for sparse implementation, in case of dense columns it is not needed to call.
     */
    abstract fun resetCursor()

    protected fun getLongFromDataSlice(elementIndex: Int): Long {
        val bytesOffset = elementIndex * elementBytesSize

        return when (type) {
            BlockType.I8Dense, BlockType.I8Sparse -> dataSlice.getByte(bytesOffset).toLong()
            BlockType.I16Dense, BlockType.I16Sparse -> dataSlice.getShort(bytesOffset).toLong()
            BlockType.I32Dense, BlockType.I32Sparse -> dataSlice.getInt(bytesOffset).toLong()
            BlockType.I64Dense, BlockType.I64Sparse -> dataSlice.getLong(bytesOffset)
            BlockType.U8Sparse, BlockType.U8Dense -> dataSlice.getUnsignedByte(bytesOffset).toLong()
            BlockType.U16Sparse, BlockType.U16Dense -> dataSlice.getUnsignedShort(bytesOffset).toLong()
            BlockType.U32Sparse, BlockType.U32Dense -> dataSlice.getUnsignedInt(bytesOffset)
            BlockType.U64Sparse, BlockType.U64Dense -> dataSlice.getLong(bytesOffset)
            BlockType.U128Sparse, BlockType.I128Sparse, BlockType.U128Dense, BlockType.I128Dense -> TODO("128bits support")
            BlockType.String -> TODO("Strings support")
        }
    }
}

/**
 * Dense column implementation.
 */
class DenseColumn : ColumnValues {
    override val type: BlockType
    override val elementsCount: Int
    override val elementBytesSize: Int
    override val dataSlice: Slice

    constructor(type: BlockType, dataSlice: Slice, size: Int) {
        this.type = type
        this.elementBytesSize = type.size().bytes
        this.elementsCount = size
        this.dataSlice = dataSlice
    }

    override fun getLong(rowId: Int): Long {
        return getLongFromDataSlice(rowId)
    }

    override fun getBytes(rowId: Int): ByteArray {
        val resultArray = ByteArray(elementBytesSize)
        dataSlice.getBytes(rowId, resultArray, 0, elementBytesSize)
        return resultArray
    }

    override fun isNull(rowId: Int): Boolean {
        return false
    }

    override fun resetCursor() {
        // NOP
    }
}

/**
 * Sparse column iterator-like implementation.
 * Only further records can be fetched (n, n+x), one cannot ask for earlier ones unless [resetCursor] is called.
 */
class SparseColumn : ColumnValues {
    override val type: BlockType
    override val elementsCount: Int
    override val elementBytesSize: Int
    override val dataSlice: Slice
    private val indexSlice: Slice
    private val indexSize = 4   // indexes are always i32
    private var currentPosition = 0

    constructor(type: BlockType, dataSlice: Slice, indexSlice: Slice, size: Int) {
        this.type = type
        this.elementBytesSize = type.size().bytes
        this.elementsCount = size
        this.dataSlice = dataSlice
        this.indexSlice = indexSlice
    }

    override fun getLong(rowId: Int): Long {
        moveCursor(rowId)
        return getLongFromDataSlice(currentPosition)
    }

    override fun getBytes(rowId: Int): ByteArray {
        moveCursor(rowId)
        val resultArray = ByteArray(elementBytesSize)
        dataSlice.getBytes(rowId, resultArray, 0, elementBytesSize)
        return resultArray
    }

    override fun isNull(rowId: Int): Boolean {
        moveCursor(rowId)
        return currentPosition >= elementsCount || indexSlice.getInt(currentPosition * indexSize) != rowId
    }

    override fun resetCursor() {
        currentPosition = 0
    }

    private fun moveCursor(offset: Int) {
        //TODO: remember next index value and compare only those to not call slice#getInt to frequently
        while (currentPosition < elementsCount && indexSlice.getInt(currentPosition * indexSize) < offset) {
            currentPosition++
        }
    }
}
