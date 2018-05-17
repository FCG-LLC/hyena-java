package co.llective.hyena.api

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.util.*

object MessageBuilderTest : Spek({
    describe("Build scan message") {
        val minTs: Long = 5
        val maxTs: Long = 10
        val partitionId1 = UUID(1, 1)
        val partitionId2 = UUID(2, 2)
        val col1: Long = 1
        val col2: Long = 2
        val numericScanFilter = ScanFilter(
                col1,
                ScanComparison.Eq,
                FilterType.U32,
                5L
        )
        val stringScanFilter = ScanFilter(
                col1,
                ScanComparison.Eq,
                FilterType.String,
                "five"
        )

        fun buildWholeScanRequest(scanFilter: ScanFilter): ScanRequest {
            return ScanRequest(
                    minTs,
                    maxTs,
                    hashSetOf(partitionId1, partitionId2),
                    ScanOrFilters(ScanAndFilters(scanFilter)),
                    listOf(col1, col2)
            )
        }

        it("builds scan message with all values") {
            val request = buildWholeScanRequest(numericScanFilter)

            val actualBytes = MessageBuilder.buildScanMessage(request)

            val expectedBytes = byteArrayOf(
                    ApiRequest.Scan.ordinal.toByte(), 0, 0, 0, // 32 bits
                    minTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits min-ts
                    maxTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits max-ts
                    2, 0, 0, 0, 0, 0, 0, 0, // 64 bits partition ids size
                    partitionId1.leastSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
                    partitionId1.mostSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
                    partitionId2.leastSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
                    partitionId2.mostSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
                    2, 0, 0, 0, 0, 0, 0, 0, // 64 bits projection list size
                    1, 0, 0, 0, 0, 0, 0, 0, // 64 bits col1 id
                    2, 0, 0, 0, 0, 0, 0, 0, // 64 bits col2 id
                    1, 0, 0, 0, 0, 0, 0, 0, // 64 bits or filters list size
                    1, 0, 0, 0, 0, 0, 0, 0, // 64 bits and filters list size
                    1, 0, 0, 0, 0, 0, 0, 0, // 64 bits scan filter column id
                    numericScanFilter.op.ordinal.toByte(), 0, 0, 0, // 32 bits scan filter operator id
                    numericScanFilter.type.ordinal.toByte(), 0, 0, 0, // 32 bits scan filter type
                    (numericScanFilter.value as Long).toByte(), 0, 0, 0 // 32 bits (u32) scan filter value
            )

            assert.that(
                    actualBytes.toList(),
                    equalTo(expectedBytes.toList())
            )
        }

        it("builds scan message with string filter") {
            val request = buildWholeScanRequest(stringScanFilter)

            val actualBytes = MessageBuilder.buildScanMessage(request)

            val stringValue = stringScanFilter.value as String

            val expectedBytes = byteArrayOf(
                    ApiRequest.Scan.ordinal.toByte(), 0, 0, 0, // 32 bits
                    minTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits min-ts
                    maxTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits max-ts
                    2, 0, 0, 0, 0, 0, 0, 0, // 64 bits partition ids size
                    partitionId1.leastSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
                    partitionId1.mostSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
                    partitionId2.leastSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
                    partitionId2.mostSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
                    2, 0, 0, 0, 0, 0, 0, 0, // 64 bits projection list size
                    1, 0, 0, 0, 0, 0, 0, 0, // 64 bits col1 id
                    2, 0, 0, 0, 0, 0, 0, 0, // 64 bits col2 id
                    1, 0, 0, 0, 0, 0, 0, 0, // 64 bits or filters list size
                    1, 0, 0, 0, 0, 0, 0, 0, // 64 bits and filters list size
                    1, 0, 0, 0, 0, 0, 0, 0, // 64 bits scan filter column id
                    stringScanFilter.op.ordinal.toByte(), 0, 0, 0,      // 32 bits scan filter operator id
                    FilterType.String.ordinal.toByte(), 0, 0, 0,        // 32 bits scan filter type
                    stringValue.length.toByte(), 0, 0, 0, 0, 0, 0, 0,   // 64 bits string value length
                    'f'.toByte(),
                    'i'.toByte(),
                    'v'.toByte(),
                    'e'.toByte()
            )

            assert.that(
                    actualBytes.toList(),
                    equalTo(expectedBytes.toList())
            )
        }

        it("builds scan message without partitions and empty projection and filter") {
            val request = buildWholeScanRequest(numericScanFilter)
            request.partitionIds = hashSetOf()
            request.projection = emptyList()
            request.filters = ScanOrFilters()

            val actualBytes = MessageBuilder.buildScanMessage(request)

            val expectedBytes = byteArrayOf(
                    ApiRequest.Scan.ordinal.toByte(), 0, 0, 0, // 32 bits
                    minTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits min-ts
                    maxTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits max-ts
                    0, 0, 0, 0, 0, 0, 0, 0, // empty hash set
                    0, 0, 0, 0, 0, 0, 0, 0, // 64 bits projection list size
                    0, 0, 0, 0, 0, 0, 0, 0 // 64 bits scan filter list size
            )

            assert.that(
                    actualBytes.toList(),
                    equalTo(expectedBytes.toList())
            )
        }
    }
})
