package co.llective.hyena.api

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.tuple.Pair
import java.util.*

open class ScanResult(
        val rowCount: Int,
        val colCount: Int,
        val colTypes: ArrayList<Pair<Int, BlockType>> = arrayListOf(),
        val blocks: List<BlockHolder> = arrayListOf())
{
    override fun toString(): String
        = "Result having $rowCount rows with $colCount columns. ${StringUtils.join(blocks, ", ")}"
}