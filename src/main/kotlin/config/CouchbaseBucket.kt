package config

data class CouchbaseBucket(
    val bucket: String,
    private val checkpointBucket: String,
    val groupName: String,
    val threshold: Long,
    val offsetCheckInitialDelayInSeconds: Long,
    val offsetCheckDelayInSeconds: Long,
) {
    fun checkpointBucket() = checkpointBucket.ifBlank { bucket }
}
