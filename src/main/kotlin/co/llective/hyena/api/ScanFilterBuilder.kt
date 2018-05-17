package co.llective.hyena.api

data class ScanFilterBuilder @JvmOverloads constructor(
        private val catalog: Catalog,
        private val filter: ScanFilter = ScanFilter.empty(FilterType.I64, 0),
        private val columnSet: Boolean = false,
        private val opSet: Boolean = false,
        private val filterValSet: Boolean = false
) {
    fun withColumn(column: Long): ScanFilterBuilder = this.copy(columnSet = true, filter = filter.copy(column = column))

    fun withOp(op: ScanComparison): ScanFilterBuilder = this.copy(opSet = true, filter = filter.copy(op = op))

    fun withValue(value: Any): ScanFilterBuilder = this.copy(filterValSet = true, filter = filter.copy(value = value))

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

        return column.dataType.mapToFilterType()
    }
}