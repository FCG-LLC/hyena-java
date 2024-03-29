package co.llective.hyena.api

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.throws
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object ScanFilterBuilderTest : Spek({
    describe("ScanFilterBuilder") {
        it("Creates ScanFilter") {
            val column = Column(BlockType.U32Dense, 10, "name")
            val catalog = Catalog(arrayListOf(column))
            val filter1 = ScanFilterBuilder(catalog)
                    .withColumn(column.id)
                    .withOp(ScanComparison.Lt)
                    .withValue(100)
                    .build()

            assert.that(filter1.column, equalTo(10L))
            assert.that(filter1.op, equalTo(ScanComparison.Lt))
            assert.that(filter1.value, equalTo(100 as Any))

            val filter2 = ScanFilterBuilder(catalog)
                    .withColumn(column.id)
                    .withOp(ScanComparison.Lt)
                    .withValue("a value")
                    .build()

            assert.that(filter2.column, equalTo(10L))
            assert.that(filter2.op, equalTo(ScanComparison.Lt))
            assert.that(filter2.value, equalTo("a value" as Any))
        }

        it("determines correctly filter type on given column") {
            val columnId: Long = 1
            val column = Column(BlockType.I64Sparse, columnId, "column name")
            val filter = ScanFilterBuilder(Catalog(arrayListOf(column)))
                    .withColumn(columnId)
                    .withOp(ScanComparison.Eq)
                    .withValue(1)
                    .build()

            assert.that(filter.type, equalTo(FilterType.I64))
        }

        it("throws when column not specified in catalog") {
            val filterBuilder = ScanFilterBuilder(Catalog())
                    .withColumn(1)
                    .withOp(ScanComparison.Lt)
                    .withValue(1)

            assert.that({ filterBuilder.build() }, throws<RuntimeException>())
        }

        it("throws when filter value not provided") {
            val filter = ScanFilterBuilder(Catalog())
                    .withColumn(10)
                    .withOp(ScanComparison.Lt)

            assert.that({ filter.build() }, throws<RuntimeException>())
        }

        it("throws when column not provided") {
            val filter = ScanFilterBuilder(Catalog())
                    .withOp(ScanComparison.Lt)
                    .withValue(100L)

            assert.that({ filter.build() }, throws<RuntimeException>())
        }

        it("throws when operation not provided") {
            val filter = ScanFilterBuilder(Catalog())
                    .withColumn(10)
                    .withValue("a value")

            assert.that({ filter.build() }, throws<RuntimeException>())
        }
    }
})