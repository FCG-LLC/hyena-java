package co.llective.hyena.util

import co.llective.hyena.api.*
import co.llective.hyena.repl.Helper
import org.apache.commons.cli.*
import java.io.File
import java.util.*
import kotlin.collections.ArrayList

class GenOptions {
    var command: String = ""
    var output: String = ""
    val columnNames: MutableList<String> = ArrayList()
    val blockTypes: MutableList<BlockType> = ArrayList()
    val columnIds: MutableList<Long> = ArrayList()
    var rows: Int = 0
    var sourceId: Int = 0
    var minTs: Long = 0
    var maxTs: Long = 0
    val partitions: MutableSet<UUID> = HashSet()
    val filterColumns: MutableList<Long> = ArrayList()
    val filterTypes: MutableList<FilterType> = ArrayList()
    val filterOperators: MutableList<ScanComparison> = ArrayList()
    val filterValues: MutableList<String> = ArrayList()
}

fun getOptions(): Options {
    val options = Options()

    options.addOption("h", "help", false, "Prints this help")
    options.addRequiredOption("c", "command", true, "Valid command (catalog, columns, addcolumn, insert, scan)")
    options.addRequiredOption("o", "output", true, "Name of a file to put output to")
    options.addOption("n", "column-name", true, "Name of the column")
    options.addOption("t", "column-type", true, "Type of the column")
    options.addOption("r", "rows", true, "Number of rows to insert")
    options.addOption("s", "source-id", true, "Source id")
    options.addOption("i", "id", true, "Id of the column")
    options.addOption("m", "min-ts", true, "Lower bound for timestamps")
    options.addOption("x", "max-ts", true, "Upper bound for timestamps")
    options.addOption("u", "uuid", false, "Add a random partition UUID")
    options.addOption("l", "filter-column", true, "Column id for the filter")
    options.addOption("f", "filter-type", true, "Filter type, one of: I8, I16, I32, I64, U8, U16, U32, U64")
    options.addOption("p", "filter-operator", true, "Filter operator, one of: LT, LTEQ, EQ, GTEQ, GT, NOTEQ")
    options.addOption("v", "filter-value", true, "Filter value")

    return options
}

fun main(args: Array<String>) {
    val parser = DefaultParser()
    val commandLine: CommandLine = try {
        parser.parse(getOptions(), args)
    } catch (exp: ParseException) {
        System.err.println("Parsing failed.  Reason: " + exp.message)
        printHelp(getOptions())
        System.exit(1)
        return
    }

    if (commandLine.hasOption("h")) {
        printHelp(getOptions())
        System.exit(0)
    }

    val options = parseOptions(commandLine)

    when (options.command) {
        "COLUMNS" -> genColumns(options)
        "CATALOG" -> genCatalog(options)
        "ADDCOLUMN" -> genAddColumns(options)
        "INSERT" -> genInsert(options)
        "SCAN" -> genScan(options)
    }
}

fun genScan(options: GenOptions) {
    if (options.filterColumns.size != options.filterTypes.size
            || options.filterColumns.size != options.filterOperators.size
            || options.filterColumns.size != options.filterValues.size) {
        println("Number of -l, -f, -p, -v must be the same")
        System.exit(1)
    }

    val filters = (0 until options.filterColumns.size).map { i ->
        ScanFilter(
                options.filterColumns[i],
                options.filterOperators[i],
                options.filterTypes[i],
                Helper.convertValue(options.filterValues[i], options.filterTypes[i]),
                Optional.empty())
    }
    val sr = ScanRequest(
            options.minTs,
            options.maxTs,
            options.partitions,
            filters,
            options.columnIds.toList()
    )
    val request = MessageBuilder.buildScanMessage(sr)
    write(options, request)
}

fun genInsert(options: GenOptions) {
    if (options.rows == 0) {
        println("Cannot insert 0 rows")
        System.exit(1)
    }
    if (options.sourceId == 0) {
        println("Need source id")
        System.exit(1)
    }
    if (options.columnIds.size == 0 || options.blockTypes.size == 0) {
        println("Need columns and they types")
        System.exit(1)
    }
    if (options.columnIds.size != options.blockTypes.size) {
        println("number of -i and -t options must match")
        System.exit(1)
    }

    val timestamps = Helper.randomTimestamps(options.rows)
    val data = (0 until options.columnIds.size).map { i ->
        ColumnData(options.columnIds[i].toInt(),
                genBlock(options.rows, options.blockTypes[i]))
    }.toList()

    val request = MessageBuilder.buildInsertMessage(options.sourceId, timestamps, data)
    write(options, request)
}

fun genBlock(rows: Int, blockType: BlockType): Block =
        if (blockType.isDense()) {
            Helper.randomDenseBlock(rows, blockType)
        } else {
            Helper.randomSparseBlock(rows, blockType)
        }

fun genAddColumns(options: GenOptions) {
    if (options.blockTypes.size < 1 || options.columnNames.size < 1) {
        println("At least one column (both name and type) are needed")
        System.exit(1)
    }
    val column = Column(options.blockTypes[0], 0, options.columnNames[0])
    val request = MessageBuilder.buildAddColumnMessage(column)
    write(options, request)
}

fun genCatalog(options: GenOptions) {
    val request = MessageBuilder.buildRefreshCatalogMessage()
    write(options, request)
}

fun genColumns(options: GenOptions) {
    val request = MessageBuilder.buildListColumnsMessage()
    write(options, request)
}

fun write(options: GenOptions, request: ByteArray) {
    val output = File(options.output)
    output.writeBytes(request)
}

fun parseOptions(commandLine: CommandLine): GenOptions {
    val options = GenOptions()

    for (option in commandLine.options) {
        when (option.opt) {
            "c" -> options.command = option.value.toUpperCase()
            "o" -> options.output = option.value
            "n" -> options.columnNames.add(option.value)
            "t" -> options.blockTypes.add(BlockType.valueOf(option.value))
            "r" -> options.rows = Integer.parseInt(option.value)
            "s" -> options.sourceId = Integer.parseInt(option.value)
            "i" -> options.columnIds.add(java.lang.Long.parseLong(option.value))
            "m" -> options.minTs = java.lang.Long.parseLong(option.value)
            "x" -> options.maxTs = java.lang.Long.parseLong(option.value)
            "u" -> options.partitions.add(UUID.randomUUID())
            "l" -> options.filterColumns.add(java.lang.Long.parseLong(option.value))
            "f" -> options.filterTypes.add(FilterType.valueOf(option.value))
            "p" -> options.filterOperators.add(ScanComparison.valueOf(option.value))
            "v" -> options.filterValues.add(option.value)
        }
    }

    if (options.filterTypes.size != options.filterValues.size) {
        println("Number of -f and -v options must be the same")
        System.exit(1)
    }

    return options
}

private fun printHelp(options: Options) {
    val formatter = HelpFormatter()
    formatter.width = 120
    formatter.printHelp("java -jar hyena-api-gentest.jar", options)
}
