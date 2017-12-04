package co.llective.hyena.api

import java.util.*

data class ScanFilterBuilder(
        private val filter: ScanFilter = ScanFilter.empty(FilterType.I64, 0),
        private val columnSet: Boolean = false,
        private val opSet: Boolean = false,
        private val filterValSet: Boolean = false
) {
    fun withColumn(column: Int): ScanFilterBuilder
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

        return filter
    }
}