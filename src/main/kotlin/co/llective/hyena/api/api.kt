package co.llective.hyena.api

import org.apache.commons.lang3.StringUtils
import java.io.DataOutput
import java.lang.IllegalArgumentException
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.*

enum class PeerRequestType {
    Request,
    Abort,
    CloseConnection,
    KeepAlive
}

enum class PeerReplyType {
    Response,
    KeepAlive
}

enum class ApiRequest {
    ListColumns,
    Insert,
    Scan,
    RefreshCatalog,
    AddColumn,
    Flush,
    DataCompaction,
    SerializeError,
    Other
}

enum class ScanComparison {
    Lt,
    LtEq,
    Eq,
    GtEq,
    Gt,
    NotEq,
    In,

    //String ones
    StartsWith,
    EndsWith,
    Contains,
    Matches
}

enum class Size(val bytes: Int) {
    Bit8(1),
    Bit16(2),
    Bit32(4),
    Bit64(8),
    Bit128(16),
    Varying(-1)
}

enum class BlockType {
    I8Dense,
    I16Dense,
    I32Dense,
    I64Dense,
    I128Dense,

    // Dense, Unsigned
    U8Dense,
    U16Dense,
    U32Dense,
    U64Dense,
    U128Dense,

    // Dense, String
    StringDense,

    // Sparse, Signed
    I8Sparse,
    I16Sparse,
    I32Sparse,
    I64Sparse,
    I128Sparse,

    // Sparse, Unsigned
    U8Sparse,
    U16Sparse,
    U32Sparse,
    U64Sparse,
    U128Sparse;

    fun mapToFilterType(): FilterType =
            when (this) {
                I8Dense, I8Sparse -> FilterType.I8
                I16Dense, I16Sparse -> FilterType.I16
                I32Dense, I32Sparse -> FilterType.I32
                I64Dense, I64Sparse -> FilterType.I64
                I128Dense, I128Sparse -> FilterType.I128
                U8Dense, U8Sparse -> FilterType.U8
                U16Dense, U16Sparse -> FilterType.U16
                U32Dense, U32Sparse -> FilterType.U32
                U64Dense, U64Sparse -> FilterType.U64
                U128Dense, U128Sparse -> FilterType.U128
                StringDense -> FilterType.String
            }

    fun isDense(): Boolean =
            when (this) {
                BlockType.I8Dense,
                BlockType.I16Dense,
                BlockType.I32Dense,
                BlockType.I64Dense,
                BlockType.I128Dense,
                BlockType.U8Dense,
                BlockType.U16Dense,
                BlockType.U32Dense,
                BlockType.U64Dense,
                BlockType.U128Dense,
                BlockType.StringDense -> true
                else -> false
            }

    fun isSparse(): Boolean = !this.isDense()

    fun size(): Size =
            when (this) {
                BlockType.I8Dense,
                BlockType.U8Dense,
                BlockType.I8Sparse,
                BlockType.U8Sparse -> Size.Bit8

                BlockType.I16Dense,
                BlockType.U16Dense,
                BlockType.I16Sparse,
                BlockType.U16Sparse -> Size.Bit16

                BlockType.I32Dense,
                BlockType.U32Dense,
                BlockType.I32Sparse,
                BlockType.U32Sparse -> Size.Bit32

                BlockType.I64Dense,
                BlockType.U64Dense,
                BlockType.I64Sparse,
                BlockType.U64Sparse -> Size.Bit64

                BlockType.I128Dense,
                BlockType.U128Dense,
                BlockType.I128Sparse,
                BlockType.U128Sparse -> Size.Bit128

                BlockType.StringDense -> Size.Varying
            }
}

sealed class PeerReply
class KeepAliveReply : PeerReply()
data class ResponseReply(val messageId: Long, val bufferPayload: ByteBuffer) : PeerReply()
data class ResponseReplyError(val messageId: Long) : PeerReply()

enum class FilterType {
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    String
}

data class ScanRequest(var minTs: Long = 0,
                       var maxTs: Long = 0,
                       var partitionIds: Set<UUID> = hashSetOf(),
                       var filters: ScanOrFilters = ScanOrFilters(),
                       var projection: List<Long> = arrayListOf())

data class ScanFilter(
        val column: Long,
        val op: ScanComparison = ScanComparison.Eq,
        val type: FilterType,
        val value: Any
) {
    override fun toString(): String = "$column ${op.name} $value"

    companion object {
        @JvmStatic
        fun empty(type: FilterType, value: Any): ScanFilter {
            return ScanFilter(0, ScanComparison.Eq, type, value)
        }
    }
}

class ScanAndFilters : ArrayList<ScanFilter> {
    constructor() : super()

    constructor(scanFilter: ScanFilter) : this() {
        add(scanFilter)
    }

    constructor(filters: List<ScanFilter>) : this() {
        filters.forEach { add(it) }
    }

    override fun toString(): String {
        val sb = StringBuilder("\tANDs:")
        forEach { sb.append("\t\t$it") }
        return sb.toString()
    }
}

class ScanOrFilters : ArrayList<ScanAndFilters> {
    constructor() : super()

    constructor(scanAndFilters: ScanAndFilters) : this() {
        add(scanAndFilters)
    }

    constructor(filters: List<ScanAndFilters>) : this() {
        filters.forEach { add(it) }
    }

    override fun toString(): String {
        val sb = StringBuilder("ORs:")
        forEach { sb.append("$it") }
        return sb.toString()
    }
}

data class ScanResult(val columnMap: MutableMap<Long, ColumnValues>)

open class Column(val dataType: BlockType, var id: Long = -1, val name: String) {
    override fun toString(): String = "$name/$id ${dataType.name}"
}

data class PartitionInfo(val minTs: Long, val maxTs: Long, val id: UUID, val location: String) {
    override fun toString(): String = "$id [$minTs-$maxTs]"
}

open class Catalog(open val columns: List<Column> = arrayListOf(),
                   open val availablePartitions: List<PartitionInfo> = arrayListOf()) {
    override fun toString(): String = "Columns: [${StringUtils.join(columns, ", ")}], " +
            "Partitions: [${StringUtils.join(availablePartitions, ", ")}]"
}

data class ColumnData(val columnIndex: Int, val block: Block) {
    override fun toString(): String = "ColumnData[id: $columnIndex, data: $block]"

}

data class BlockHolder(val type: BlockType, val block: Block) {
    override fun toString(): String = "$type with ${block.count()} elements"

    fun printNumbers(): String = block.printNumbers()
}

class ReplyException : Exception {
    constructor(s: String) : super(s)
    constructor(s: String, cause: ApiError) : super(s + " - " + cause.type + " (" + cause.extra.orElse(" ") + ")")
    constructor(cause: ApiError) : this("Api exception occurred", cause)
}

abstract class Block(val type: BlockType) {

    abstract fun write(dos: DataOutput)

    abstract fun count(): Int

    abstract fun printNumbers(): String

    fun <T> write(dos: DataOutput, item: T) {
        when (item) {
            is Byte -> dos.writeByte(item.toInt())
            is Short -> dos.writeShort(item.toInt())
            is Int -> dos.writeInt(item)
            is Long -> dos.writeLong(item)
            is BigInteger -> writeBigInteger(dos, item)
        }
    }

    fun <T : Number> writeValue(dos: DataOutput, item: T) {
        when (type.size()) {
            Size.Bit8 -> dos.writeByte(item.toInt())
            Size.Bit16 -> dos.writeShort(item.toInt())
            Size.Bit32 -> dos.writeInt(item.toInt())
            Size.Bit64 -> dos.writeLong(item.toLong())
            Size.Bit128 -> writeBigInteger(dos, item as BigInteger)
            Size.Varying -> TODO()
        }
    }

    internal fun writeBigInteger(dos: DataOutput, item: BigInteger) {
        if (type == BlockType.U64Dense || type == BlockType.U64Sparse) {
            MessageBuilder.writeU64(dos, item)
        } else {
            throw RuntimeException("Cannot serialize BigInteger for types other then U64Dense and U64Sparse")
        }
    }
}

open class DenseBlock<T : Number> : Block {
    val data: ArrayList<T>

    private constructor(type: BlockType, data: ArrayList<T>) : super(type) {
        this.data = data
    }

    constructor(type: BlockType, size: Int) : this(type = type, data = ArrayList<T>(size)) {
        if (size < 0) {
            throw IllegalArgumentException("Data size must not be negative")
        }

        when (type) {
            BlockType.I8Sparse,
            BlockType.I16Sparse,
            BlockType.I32Sparse,
            BlockType.I64Sparse,
            BlockType.U8Sparse,
            BlockType.U16Sparse,
            BlockType.U32Sparse,
            BlockType.U64Sparse ->
                throw IllegalArgumentException("Can't create a dense block with sparse data type")
            BlockType.StringDense ->
                throw IllegalArgumentException("Can't create a dense block with String data type")
            else -> { /* OK, do nothing */
            }
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
            writeValue(dos, item)
        }
    }

    override fun toString(): String = "$data"

    override fun count(): Int = data.size

    override fun printNumbers(): String {
        return if (data.isEmpty()) {
            "Empty"
        } else {
            data.foldIndexed(StringBuffer(), { i, sb, v -> sb.append("$i: $v, ") })
                    .toString()
        }
    }
}

class SparseBlock<T : Number> : Block {
    private var currentPosition = 0
    val offsetData: MutableList<Int>
    val valueData: MutableList<T>

    private constructor(type: BlockType, offsetData: MutableList<Int>, valueData: MutableList<T>)
            : super(type) {
        this.offsetData = offsetData
        this.valueData = valueData
    }

    constructor(type: BlockType, size: Int)
            : this(type = type, offsetData = ArrayList(size), valueData = ArrayList(size)) {
        if (size < 0) {
            throw IllegalArgumentException("Data size must not be negative")
        }

        when (type) {
            BlockType.I8Dense,
            BlockType.I16Dense,
            BlockType.I32Dense,
            BlockType.I64Dense,
            BlockType.U8Dense,
            BlockType.U16Dense,
            BlockType.U32Dense,
            BlockType.U64Dense ->
                throw IllegalArgumentException("Can't create a dense block with dense data type")
            BlockType.StringDense ->
                throw IllegalArgumentException("Can't create a dense block with String data type")
            else -> { /* OK, do nothing */
            }
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

    fun add(offset: Int, value: T): SparseBlock<T> {
        offsetData.add(offset)
        valueData.add(value)
        return this
    }

    override fun write(dos: DataOutput) {
        dos.writeLong(valueData.size.toLong())
        for (item in valueData) {
            writeValue(dos, item)
        }

        dos.writeLong(offsetData.size.toLong())
        for (item in offsetData) {
            write(dos, item)
        }
    }

    override fun count(): Int = offsetData.size

    override fun printNumbers(): String =
            offsetData.zip(valueData)
                    .fold("", { str, (o, v) -> "$str, $o/$v" })
}

class StringBlock : Block {
    override fun write(dos: DataOutput) {
        dos.writeLong(strings.size.toLong())
        for (string in strings) {
            dos.writeLong(string.length.toLong())
            dos.writeBytes(string)
        }
    }

    val strings: MutableList<String>

    private constructor(strings: MutableList<String>)
            : super(BlockType.StringDense) {
        this.strings = strings
    }

    constructor(size: Int)
            : this(strings = ArrayList<String>(size)) {
        if (size <= 0) {
            throw IllegalArgumentException("Data size must be positive")
        }
    }

    fun add(string: String) {
        strings.add(string)
    }

    override fun count(): Int = strings.size

    override fun printNumbers(): String = "Not implemented"
}