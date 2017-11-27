package co.llective.hyena.api

import co.llective.hyena.api.HyenaApi.Companion.UTF8_CHARSET
import org.apache.commons.lang3.StringUtils
import java.io.DataOutput
import java.lang.IllegalArgumentException
import java.math.BigInteger
import java.util.*

enum class ApiRequest {
    ListColumns,
    Insert,
    Scan,
    RefreshCatalog,
    AddColumn,
    Flush,
    DataCompaction,
    SerializeError,
    CatalogError
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
                       var partitionId: UUID = UUID.randomUUID(),
                       var filters: List<ScanFilter> = arrayListOf(),
                       var projection: List<Int> = arrayListOf())

data class ScanFilter(
        val column: Int,
        val op: ScanComparison = ScanComparison.Eq,
        val value: Long,
        val strValue: Optional<String>
) {
    override fun toString(): String = "$column ${op.name} $value/$strValue"

    companion object {
        @JvmStatic
        fun empty(): ScanFilter {
            return ScanFilter(0, ScanComparison.Eq, 0, Optional.empty())
        }
    }
}

open class Column(val dataType: BlockType, val id: Int, val name: String) {
    override fun toString(): String = "$name/$id ${dataType.name}"
}

data class PartitionInfo(val minTs: Long, val maxTs: Long, val id: UUID, val location: String) {
    override fun toString(): String = "$id [$minTs-$maxTs]"
}

open class Catalog(val columns: List<Column> = arrayListOf(),
              val availablePartitions: List<PartitionInfo> = arrayListOf())
{
    override fun toString(): String
        = "Columns: [${StringUtils.join(columns, ", ")}], " +
          "Partitions: [${StringUtils.join(availablePartitions, ", ")}]"
}

data class ColumnData(val columnIndex: Int, val block: Block) {
    override fun toString(): String = "ColumnData[id: $columnIndex, data: $block]"

}

data class BlockHolder(val type: BlockType, val block: Block) {
    override fun toString(): String = "$type with ${block.count} elements"
}

class ReplyException(s: String) : Exception(s)

abstract class Block(val type: BlockType, val count: Int) {

    abstract fun write(dos: DataOutput)

    fun <T> write(dos: DataOutput, item: T) {
        when (item) {
            is Byte  -> dos.writeByte(item.toInt())
            is Short -> dos.writeShort(item.toInt())
            is Int   -> dos.writeInt(item)
            is Long  -> dos.writeLong(item)
            is BigInteger -> writeBigInteger(dos, item)
        }
    }

    private val TWO_COMPLEMENT: BigInteger = BigInteger.ONE.shiftLeft(64);
    private val MAX_LONG_BI: BigInteger = BigInteger.valueOf(Long.MAX_VALUE);

    internal fun writeBigInteger(dos: DataOutput, item: BigInteger) {
        if (type == BlockType.U64Dense || type == BlockType.U64Sparse) {
            val bi = if (item <= MAX_LONG_BI) {item} else { item - TWO_COMPLEMENT }
            dos.writeLong(bi.longValueExact())
        } else {
            throw RuntimeException("Cannot serialize BigInteger for types other then U64Dense and U64Sparse")
        }
    }
}

open class DenseBlock<T> : Block {
    val data: ArrayList<T>

    private constructor(type: BlockType, data: ArrayList<T>) : super(type, data.size) { this.data = data}
    constructor(type: BlockType, size: Int): this(type = type, data = ArrayList<T>(size))
    {
        if (size <= 0) {
            throw IllegalArgumentException("Data size must be positive")
        }

        when(type) {
            BlockType.I8Sparse,
            BlockType.I16Sparse,
            BlockType.I32Sparse,
            BlockType.I64Sparse,
            BlockType.U8Sparse,
            BlockType.U16Sparse,
            BlockType.U32Sparse,
            BlockType.U64Sparse ->
                throw IllegalArgumentException("Can't create a dense block with sparse data type")
            BlockType.String ->
                throw IllegalArgumentException("Can't create a dense block with String data type")
            else -> { /* OK, do nothing */ }
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun add(value: Any): DenseBlock<T> {
        data.add(value as T)
        return this
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
            : this(type = type, offsetData = ArrayList(size), valueData = ArrayList(size))
    {
        if (size <= 0) {
            throw IllegalArgumentException("Data size must be positive")
        }

        when(type) {
            BlockType.I8Dense,
            BlockType.I16Dense,
            BlockType.I32Dense,
            BlockType.I64Dense,
            BlockType.U8Dense,
            BlockType.U16Dense,
            BlockType.U32Dense,
            BlockType.U64Dense ->
                throw IllegalArgumentException("Can't create a dense block with dense data type")
            BlockType.String ->
                throw IllegalArgumentException("Can't create a dense block with String data type")
            else -> { /* OK, do nothing */ }
        }
    }

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
    fun add(offset: Int, value: Any): SparseBlock<T> {
        offsetData.add(offset)
        valueData.add(value as T)
        return this
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
            : this(offsetData = ArrayList<Int>(size), valueStartPositions = ArrayList<Long>(size)) {
        if (size <= 0) {
            throw IllegalArgumentException("Data size must be positive")
        }
    }

    fun getMaybe(offset: Int): Optional<String> {
        while (currentPosition < offsetData.size && offsetData[currentPosition] < offset) {
            currentPosition++
        }

        if (currentPosition < offsetData.size && offsetData[currentPosition] == offset) {
            val startPosition = valueStartPositions[currentPosition].toInt()
            val endPosition = if (currentPosition == offsetData.size - 1) {
                bytes!!.size
            } else {
                valueStartPositions[currentPosition + 1].toInt()
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