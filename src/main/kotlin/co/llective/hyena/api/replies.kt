package co.llective.hyena.api

import java.util.*

sealed class Either<out L, out R>
data class Left<out L>(val value: L) : Either<L, Nothing>()
data class Right<out R>(val value: R) : Either<Nothing, R>()

enum class ApiErrorType(val type: ExtraType) {
    ColumnNameAlreadyExists(ExtraType.String),
    ColumnIdAlreadyExists(ExtraType.Long),
    Unknown(ExtraType.String);

    enum class ExtraType {
        Long,
        String,
        None
    }
}
data class ApiError(val type: ApiErrorType, val extra: Optional<Any>) { }

open class Reply {}
class ListColumnsReply(val columns: List<Column>) : Reply() { }
class AddColumnReply(val result: Either<Int, ApiError>) : Reply() {}