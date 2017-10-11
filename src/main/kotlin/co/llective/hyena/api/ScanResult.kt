package co.llective.hyena.api

import io.airlift.log.Logger
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.tuple.Pair
import java.io.IOException
import java.nio.ByteBuffer
import java.util.ArrayList

class ScanResult(val rowCount: Int,
                 val colCount: Int,
                 val colTypes: List<Pair<Int, BlockType>> = arrayListOf(),
                 val blocks: List<BlockHolder> = arrayListOf())
{
    override fun toString(): String {
        val sb = StringBuilder()

        sb.append(String.format("Result having %d rows with %d columns. ", rowCount, colCount))
        sb.append(StringUtils.join(blocks, ", "))
        return sb.toString()
    }

    companion object {
        private val log = Logger.get(ScanResult::class.java)

        @Throws(IOException::class)
        private fun decodeColumnTypes(buf: ByteBuffer): List<Pair<Int, BlockType>> {
            val colCount = buf.long
            val colTypes = ArrayList<Pair<Int, BlockType>>()

            for (i in 0 until colCount) {
                colTypes.add(Pair.of(buf.int, BlockType.values()[buf.int]))
            }

            return colTypes
        }

        @Throws(IOException::class)
        private fun decodeBlocks(buf: ByteBuffer): List<BlockHolder> {
            val count = buf.long
            val blocks = ArrayList<BlockHolder>()
            for (i in 0 until count) {
                blocks.add(BlockHolder.decode(buf))
            }
            return blocks
        }

        @Throws(IOException::class)
        fun decode(buf: ByteBuffer): ScanResult {
            val rowCount = buf.int
            val colCount = buf.int

            log.info("Received ResultSet with %d rows", rowCount)

            return ScanResult(
                    rowCount,
                    colCount,
                    decodeColumnTypes(buf),
                    decodeBlocks(buf)
            )
        }
    }
}