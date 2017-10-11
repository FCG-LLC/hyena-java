package co.llective.hyena.api

data class ScanFilterBuilder(val filter: ScanFilter, val columnSet: Boolean, val opSet: Boolean, val filterValSet: Boolean) {

    fun withColumn(column: Int): ScanFilterBuilder
        = this.copy(columnSet = true, filter = filter.copy(column = column))

    fun withOp(op: ScanComparison): ScanFilterBuilder
        = this.copy(opSet = true, filter = filter.copy(op = op))

    fun withLongValue(value: Long): ScanFilterBuilder
        = this.copy(filterValSet = true, filter = filter.copy(value = value))

    fun withStringValue(value: String): ScanFilterBuilder
        = this.copy(filterValSet = true, filter = filter.copy(strValue = value))

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

        return filter
    }
}