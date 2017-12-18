package co.llective.hyena.api

import java.util.*

data class ScanFilterBuilder @JvmOverloads constructor(
        private val catalog: Catalog,
        private val filter: ScanFilter = ScanFilter.empty(FilterType.I64, 0),
        private val columnSet: Boolean = false,
        private val opSet: Boolean = false,
        private val filterValSet: Boolean = false
) {
    fun withColumn(column: Long): ScanFilterBuilder
            = this.copy(columnSet = true, filter = filter.copy(column = column))

    fun withOp(op: ScanComparison): ScanFilterBuilder
            = this.copy(opSet = true, filter = filter.copy(op = op))

    fun withValue(value: Any): ScanFilterBuilder
            = this.copy(filterValSet = true, filter = filter.copy(value = value))

    fun withStringValue(value: String): ScanFilterBuilder
            = this.copy(filterValSet = true, filter = filter.copy(strValue = Optional.of(value)))

    fun build(): ScanFilter {
        if (!filterValSet) {
            throw RuntimeException("Filter value not set")
        }

        if (!columnSet) {
            throw RuntimeException("Column not set")
        }

        if (!opSet) {
            throw RuntimeException("Operation not set")
        }

        val type: FilterType = determineFilterType()

        return filter.copy(type = type)
    }

    private fun determineFilterType(): FilterType {
        val column = catalog.columns.findLast { col -> col.id == filter.column } ?: throw RuntimeException("Column not found in catalog")

        return when (column.dataType) {
            BlockType.I8Dense, BlockType.I8Sparse -> FilterType.I8
            BlockType.U8Dense, BlockType.U8Sparse -> FilterType.U8
            BlockType.I16Dense, BlockType.I16Sparse -> FilterType.I16
            BlockType.U16Dense, BlockType.U16Sparse -> FilterType.U16
            BlockType.I32Dense, BlockType.I32Sparse -> FilterType.I32
            BlockType.U32Dense, BlockType.U32Sparse -> FilterType.U32
            BlockType.I64Dense, BlockType.I64Sparse -> FilterType.I64
            BlockType.U64Dense, BlockType.U64Sparse -> FilterType.U64
            BlockType.I128Dense, BlockType.I128Sparse -> FilterType.I128
            BlockType.U128Dense, BlockType.U128Sparse -> FilterType.U128
            BlockType.String -> FilterType.String
        }
    }
}