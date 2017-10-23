package co.llective.hyena.api

import co.llective.hyena.api.HyenaApi.Companion.UTF8_CHARSET
import org.apache.commons.lang3.StringUtils
import java.io.DataOutput
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

data class ScanRequest(var minTs: Long = 0,
                       var maxTs: Long = 0,
                       var partitionId: Long = 0,
                       var filters: List<ScanFilter> = arrayListOf(),
                       var projection: List<Int> = arrayListOf()) {}

data class ScanFilter(val column: Int, val op: ScanComparison = ScanComparison.Eq, val value: Long, val strValue: String) {
    override fun toString(): String = "$column ${op.name} $value/$strValue"
}

data class Column(val dataType: BlockType, val id: Int, val name: String) {
    override fun toString(): String = "$name/$id ${dataType.name}"
}

data class PartitionInfo(val minTs: Long, val maxTs: Long, val id: UUID, val location: String) {
    override fun toString(): String = "$id [$minTs-$maxTs]"
}

class Catalog(val columns: List<Column> = arrayListOf(),
              val availablePartitions: List<PartitionInfo> = arrayListOf())
{
    override fun toString(): String {
        val sb = StringBuilder()
        sb.append("Columns: [")
        sb.append(StringUtils.join(columns, ", "))
        sb.append("], Partitions: [")
        sb.append(StringUtils.join(availablePartitions, ", "))
        sb.append("]")
        return sb.toString()
    }
}

data class ColumnData(val columnIndex: Int, val block: Block) {
    override fun toString(): String = "ColumnData[id: $columnIndex, data: $block]"

}

abstract class Block(val type: BlockType, val count: Int) {

    abstract fun write(dos: DataOutput)

    protected fun <T> write(dos: DataOutput, item: T) {
        when (item) {
            is Byte  -> dos.writeByte(item.toInt())
            is Short -> dos.writeShort(item.toInt())
            is Int   -> dos.writeInt(item)
            is Long  -> dos.writeLong(item)
        }
    }
}

class DenseBlock<T> : Block {
    val data: ArrayList<T>

    private constructor(type: BlockType, data: ArrayList<T>) : super(type, data.size) { this.data = data}
    constructor(type: BlockType, size: Int): this(type = type, data = ArrayList<T>(size)) { }

    @Suppress("UNCHECKED_CAST")
    fun add(value: Any) {
        data.add(value as T)
    }

    override fun write(dos: DataOutput) {
        dos.writeLong(data.size.toLong())
        for (item in data) {
            write(dos, item)
        }
    }

    override fun toString(): String = "$data"
}

class SparseBlock<T> : Block
{
    private var currentPosition = 0
    val offsetData: MutableList<Int>
    val valueData: MutableList<T>

    private constructor(type: BlockType, offsetData: MutableList<Int>, valueData: MutableList<T>)
        : super(type, offsetData.size)
    {
        this.offsetData = offsetData
        this.valueData = valueData
    }

    constructor(type: BlockType, size: Int)
            : this(type = type, offsetData = ArrayList(size), valueData = ArrayList(size)) {}

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

    override fun write(dos: DataOutput) {
        dos.writeLong(valueData.size.toLong())
        for (item in valueData) {
            write(dos, item)
        }

        dos.writeLong(offsetData.size.toLong())
        for (item in offsetData) {
            write(dos, item)
        }
    }
}

class StringBlock : Block
{
    override fun write(dos: DataOutput) {
        TODO("not implemented") //Will be implemented when Hyena handles it
    }

    var bytes: ByteArray? = null
    val offsetData: MutableList<Int>
    val valueStartPositions: MutableList<Long>

    private var currentPosition = 0

    private constructor(offsetData: MutableList<Int>, valueStartPositions: MutableList<Long>)
        : super(BlockType.String, offsetData.size) {
        this.offsetData = offsetData
        this.valueStartPositions = valueStartPositions
    }
    constructor(size: Int)
            : this(offsetData = ArrayList<Int>(size), valueStartPositions = ArrayList<Long>(size)) {}

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