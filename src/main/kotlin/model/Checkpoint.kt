package model

data class Checkpoint(
    val vbuuid: Long, val seqno: Long, val snapshot: SnapshotMarker
)

data class SnapshotMarker(val startSeqno: Long, val endSeqno: Long)
