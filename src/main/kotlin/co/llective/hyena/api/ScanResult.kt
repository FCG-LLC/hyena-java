package co.llective.hyena.api

import io.airlift.log.Logger
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.tuple.Pair
import java.io.IOException
import java.nio.ByteBuffer
import java.util.ArrayList

class ScanResult(val rowCount: Int,
                 val colCount: Int,
                 val colTypes: ArrayList<Pair<Int, BlockType>> = arrayListOf(),
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
    }
}