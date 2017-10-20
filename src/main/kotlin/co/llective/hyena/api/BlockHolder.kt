package co.llective.hyena.api

import java.io.IOException
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.*

class BlockHolder {
    var type: BlockType? = null
    var block: Optional<Block> = Optional.empty()

    override fun toString(): String {
        val count = if (block.isPresent) { block.get().count } else 0
        return String.format("%s with %d elements", type!!.name, count)
    }
}