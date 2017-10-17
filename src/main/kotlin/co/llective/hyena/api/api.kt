package co.llective.hyena.api

import co.llective.hyena.api.HyenaApi.Companion.UTF8_CHARSET
import org.apache.commons.lang3.StringUtils
import java.util.*

enum class ApiRequest {
    ListColumns,
    Insert,
    Scan,
    RefreshCatalog,
    AddColumn,
    Flush,
    DataCompaction
}

enum class ScanComparison {
    Lt,
    LtEq,
    Eq,
    GtEq,
    Gt,
    NotEq
}

enum class BlockType {
    I8Dense,
    I16Dense,
    I32Dense,
    I64Dense,
    // I128Dense,

    // Dense, Unsigned
    U8Dense,
    U16Dense,
    U32Dense,
    U64Dense,
    // U128Dense,

    // Sparse, Signed
    I8Sparse,
    I16Sparse,
    I32Sparse,
    I64Sparse,
    // I128Sparse,

    // Sparse, Unsigned
    U8Sparse,
    U16Sparse,
    U32Sparse,
    U64Sparse,
    // U128Sparse,
    String
}

class ScanRequest {
    var minTs: Long = 0
    var maxTs: Long = 0
    var partitionId: Long = 0
    var filters: List<ScanFilter> = arrayListOf()
    var projection: List<Int> = arrayListOf()
}

data class ScanFilter(val column: Int, val op: ScanComparison = ScanComparison.Eq, val value: Long, val strValue: String) {
    override fun toString(): String = String.format("%d %s %d/%s", column, op.name, value, strValue)
}

data class Column(val dataType: BlockType, val id: Int, val name: String) {
    override fun toString(): String = "$name/$id ${dataType.name}"
}

data class PartitionInfo(val minTs: Long, val maxTs: Long, val id: Long, val location: String) {
    override fun toString(): String = String.format("%d [%d-%d]", this.id, this.minTs, this.maxTs)
}

class Catalog(val columns: List<Column> = arrayListOf(),
              val availablePartitions: List<PartitionInfo> = arrayListOf())
{
    override fun toString(): String {
        val sb = StringBuilder()
        sb.append("Columns: ")
        sb.append(StringUtils.join(columns, ", "))
        sb.append("Partitions: ")
        sb.append(StringUtils.join(availablePartitions, ", "))
        return sb.toString()
    }
}

interface Block {
    val count: Int
}

class DenseBlock<T>(val data: ArrayList<T>) : Block {
    override val count = data.size
    constructor(size: Int): this(data = ArrayList<T>(size)) { }

    @Suppress("UNCHECKED_CAST")
    fun add(value: Any) {
        data.add(value as T)
    }
}

class SparseBlock<T>(val offsetData: MutableList<Int>, val valueData: MutableList<T>) : Block
{
    private var currentPosition = 0

    override val count = offsetData.size

    constructor(size: Int): this(offsetData = ArrayList(size), valueData = ArrayList(size)) {}

    fun getMaybe(offset: Int): Optional<T> {
        while (currentPosition < offsetData.size && offsetData[currentPosition] < offset) {
            currentPosition++
        }

        return if (currentPosition < offsetData.size && offsetData[currentPosition] == offset) {
            Optional.of(valueData[currentPosition])
        } else
            Optional.empty()
    }

    @Suppress("UNCHECKED_CAST")
    fun add(offset: Int, value: Any) {
        offsetData.add(offset)
        valueData.add(value as T)
    }
}

class StringBlock(val offsetData: MutableList<Int>,
                  val valueStartPositions: MutableList<Long>) : Block
{
    var bytes: ByteArray? = null

    override val count = offsetData.size

    private var currentPosition = 0

    constructor(size: Int): this(offsetData = ArrayList<Int>(size), valueStartPositions = ArrayList<Long>(size)) {}

    fun getMaybe(offset: Int): Optional<String> {
        while (currentPosition < offsetData.size && offsetData[currentPosition] < offset) {
            currentPosition++
        }

        if (currentPosition < offsetData.size && offsetData[currentPosition] == offset) {
            val startPosition = valueStartPositions[currentPosition].toInt()
            val endPosition: Int

            if (currentPosition == offsetData.size - 1) {
                endPosition = bytes!!.size
            } else {
                endPosition = valueStartPositions[currentPosition + 1].toInt()
            }

            val strBytes = Arrays.copyOfRange(bytes!!, startPosition, endPosition)
            return Optional.of(String(strBytes, UTF8_CHARSET))
        }

        return Optional.empty()
    }

    fun add(offset: Int, value: Long) {
        offsetData.add(offset)
        valueStartPositions.add(value)
    }
}