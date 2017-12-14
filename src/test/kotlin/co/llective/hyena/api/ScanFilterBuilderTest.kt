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
            val filter1 = ScanFilterBuilder()
                    .withColumn(10)
                    .withOp(ScanComparison.Lt)
                    .withValue(100)
                    .build()

            assert.that(filter1.column, equalTo(10L))
            assert.that(filter1.op, equalTo(ScanComparison.Lt))
            assert.that(filter1.value, equalTo(100 as Any))
            assert.that(filter1.strValue.isPresent, equalTo(false))

            val filter2 = ScanFilterBuilder()
                    .withColumn(10)
                    .withOp(ScanComparison.Lt)
                    .withStringValue("a value")
                    .build()

            assert.that(filter2.column, equalTo(10L))
            assert.that(filter2.op, equalTo(ScanComparison.Lt))
            assert.that(filter2.value, equalTo(0 as Any))
            assert.that(filter2.strValue.isPresent, equalTo(true))
            assert.that(filter2.strValue.get(), equalTo<String?>("a value"))
        }

        it("throws when filter value not provided") {
            val filter = ScanFilterBuilder()
                    .withColumn(10)
                    .withOp(ScanComparison.Lt)

            assert.that({filter.build()}, throws<RuntimeException>())
        }

        it("throws when column not provided") {
            val filter = ScanFilterBuilder()
                    .withOp(ScanComparison.Lt)
                    .withValue(100L)

            assert.that({filter.build()}, throws<RuntimeException>())
        }

        it("throws when operation not provided") {
            val filter = ScanFilterBuilder()
                    .withColumn(10)
                    .withStringValue("a value")

            assert.that({filter.build()}, throws<RuntimeException>())
        }
    }
})