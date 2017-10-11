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
    Int64Dense,
    Int64Sparse,
    Int32Sparse,
    Int16Sparse,
    Int8Sparse,
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

data class Column(val dataType: BlockType, val name: String) {
    override fun toString(): String = String.format("%s %s", name, dataType.name)
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

class DenseBlock<T>(val data: ArrayList<T>) : List<T> by data {
    constructor(size: Int): this(data = ArrayList(size)) { }
}

class SparseBlock<T>(val offsetData: MutableList<Int>, val valueData: MutableList<T>)
{
    private var currentPosition = 0

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
}

class StringBlock(val offsetData: MutableList<Int>,
                  val valueStartPositions: MutableList<Long>)
{
    var bytes: ByteArray? = null

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
}