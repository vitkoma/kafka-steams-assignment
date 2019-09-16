package koma.homework.model

object OpType {
    const val CREATE = "c"
    const val UPDATE = "u"
    const val DELETE = "d"
}

data class ModEvent (
    val op: String,
    val after: String?,
    val patch: String?
)

data class TGroup (
    val taskId: String,
    val tGroupId: Int,
    val levels: List<Int>?,
    val tUnits: List<TUnit>
)

data class TUnit (
    val tUnitId: String,
    val confirmedLevel: Int
)