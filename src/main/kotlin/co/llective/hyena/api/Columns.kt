package co.llective.hyena.api

import io.airlift.slice.Slice

/**
 * Contains data for given column.
 */
abstract class SlicedColumn {
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
        return when (type.size()) {
            Size.Bit8 -> dataSlice.getByte(bytesOffset).toLong()
            Size.Bit16 -> dataSlice.getShort(bytesOffset).toLong()
            Size.Bit32 -> dataSlice.getInt(bytesOffset).toLong()
            Size.Bit64 -> dataSlice.getLong(bytesOffset)
            else -> throw java.lang.IllegalArgumentException("Not supported type size: ${type.size()}")
        }
    }
}

/**
 * Dense column implementation.
 */
class DenseColumn : SlicedColumn {
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
class SparseColumn : SlicedColumn {
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
