package co.llective.hyena.api

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.util.Optional
import java.util.UUID

object MessageBuilderTest : Spek({
  describe("Build scan message") {
    val minTs : Long = 5
    val maxTs : Long = 10
    val partitionId1 = UUID(1, 1)
    val partitionId2 = UUID(2, 2)
    val col1 : Long = 1
    val col2 : Long = 2
    val scanFilter1 = ScanFilter(
        col1,
        ScanComparison.Eq,
        FilterType.U32,
        5L,
        Optional.empty()
    )

    fun buildWholeScanRequest() : ScanRequest {
      return ScanRequest(
          minTs,
          maxTs,
          hashSetOf(partitionId1, partitionId2),
          listOf(scanFilter1),
          listOf(col1, col2)
      )
    }

    it("builds scan message with all values") {
      val request = buildWholeScanRequest()

      val actualBytes = MessageBuilder.buildScanMessage(request)

      val expectedBytes = byteArrayOf(
          ApiRequest.Scan.ordinal.toByte(), 0, 0, 0, // 32 bits
          minTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits min-ts
          maxTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits max-ts
          1, // boolean optional - true
          2, 0, 0, 0, 0, 0, 0, 0, // 64 bits partition ids size
          partitionId1.leastSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
          partitionId1.mostSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
          partitionId2.leastSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
          partitionId2.mostSignificantBits.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits uuid
          2, 0, 0, 0, 0, 0, 0, 0, // 64 bits projection list size
          1, 0, 0, 0, 0, 0, 0, 0, // 64 bits col1 id
          2, 0, 0, 0, 0, 0, 0, 0, // 64 bits col2 id
          1, 0, 0, 0, 0, 0, 0, 0, // 64 bits scan list size
          1, 0, 0, 0, 0, 0, 0, 0, // 64 bits scan column id
          scanFilter1.op.ordinal.toByte(), 0, 0, 0, // 32 bits
          scanFilter1.type.ordinal.toByte(), 0, 0, 0, // 32 bits
          (scanFilter1.value as Long).toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits
          0, 0, 0, 0, 0, 0, 0, 0 // 64 bits strange 0 no-string value number
      )

      assert.that(
          actualBytes.toList(),
          equalTo(expectedBytes.toList())
      )
    }

    it("builds scan message without partitions and empty projection and filter") {
      val request = buildWholeScanRequest()
      request.partitionIds = null
      request.projection = emptyList()
      request.filters = emptyList()

      val actualBytes = MessageBuilder.buildScanMessage(request)

      val expectedBytes = byteArrayOf(
          ApiRequest.Scan.ordinal.toByte(), 0, 0, 0, // 32 bits
          minTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits min-ts
          maxTs.toByte(), 0, 0, 0, 0, 0, 0, 0, // 64 bits max-ts
          0, // boolean optional - false
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
