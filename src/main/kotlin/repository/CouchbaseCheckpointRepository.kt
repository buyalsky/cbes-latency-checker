package repository

import com.couchbase.client.java.Collection
import com.couchbase.client.java.ReactiveCollection
import com.couchbase.client.java.kv.LookupInResult
import com.couchbase.client.java.kv.LookupInSpec
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.gson.internal.`$Gson$Preconditions`.checkArgument
import helper.MarkableCrc32
import model.Checkpoint
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.*

class CouchbaseCheckpointRepository(collection: Collection, groupName: String, numPartitions: Int) {
    private val reactiveCollection: ReactiveCollection
    private val checkpointDocumentKeys: List<String>
    private val vBucketSet: Set<Int>
    private val groupName: String

    companion object {
        private const val XATTR_NAME = "cbes"
        private const val METADATA_DOCUMENT_ID_PREFIX = "_connector:cbes:"
        private val logger = LoggerFactory.getLogger(CouchbaseCheckpointRepository::class.java)
        private val mapper = jacksonObjectMapper()
    }

    init {
        reactiveCollection = collection.reactive()
        vBucketSet = (0 until numPartitions).toList().toSet()
        this.groupName = groupName


        val keyPrefix = METADATA_DOCUMENT_ID_PREFIX + this.groupName + ":checkpoint:"

        checkpointDocumentKeys = (0 until numPartitions).map { partition ->
            val baseKey = keyPrefix + partition
            return@map forceKeyToPartition(baseKey, partition, numPartitions).orElse(baseKey)
        }
    }

    fun loadCheckpointDocuments(): Map<Int, Long> {
        val results = Flux.fromIterable(vBucketSet).flatMap { vBucket: Int ->
            lookupIn(
                vBucket
            )
        }.onErrorContinue { throwable: Throwable, _: Any? -> throwable.printStackTrace() }.collectList().block()!!

        val result = results.parallelStream().map { (vBucket: Int, lookupInResult: LookupInResult) ->
            try {
                val jsonNode = mapper.readTree(lookupInResult.contentAs(0, ByteArray::class.java))
                val checkpoint = mapper.convertValue(jsonNode.get("checkpoint"), Checkpoint::class.java)
                return@map Optional.of(Pair(vBucket, checkpoint.seqno))
            } catch (e: IOException) {
                logger.warn("Error while getting checkpoint documents, err: " + e.message)
                return@map Optional.empty()
            }
        }.filter { it.isPresent }.toList().associate { it.get() }

        val numberOfUncompletedTasks: Int = vBucketSet.size - result.size
        if (numberOfUncompletedTasks > 0) {
            logger.warn(String.format("%d tasks are not completed due to timeout", numberOfUncompletedTasks))
        }

        return result

    }

    private fun lookupIn(vBucket: Int): Mono<Pair<Int, LookupInResult>> {
        val checkpointDocumentKey = checkpointDocumentKeys[vBucket]
        val lookupInResultMono = reactiveCollection.lookupIn(
            checkpointDocumentKey, listOf<LookupInSpec>(
                LookupInSpec.get(
                    XATTR_NAME
                ).xattr()
            )
        )
        return lookupInResultMono.map { lookupInResult: LookupInResult ->
            Pair(
                vBucket, lookupInResult
            )
        }
    }


    fun getNumPartitions(): Int = this.vBucketSet.size
    fun getGroupName(): String = this.groupName

}

fun forceKeyToPartition(key: String, desiredPartition: Int, numPartitions: Int): Optional<String> {
    checkArgument(desiredPartition < numPartitions)
    checkArgument(numPartitions > 0)
    checkArgument(desiredPartition >= 0)

    val MAX_ITERATIONS = 10000000
    val crc32 = MarkableCrc32()
    val keyBytes = "$key#".toByteArray(StandardCharsets.UTF_8)
    crc32.update(keyBytes, 0, keyBytes.size)
    crc32.mark()
    for (salt in 0 until MAX_ITERATIONS) {
        crc32.reset()
        val saltString = java.lang.Long.toHexString(salt.toLong())
        var i = 0
        val max = saltString.length
        while (i < max) {
            crc32.update(saltString[i].code)
            i++
        }
        val rv: Long = crc32.value shr 16 and 0x7fff
        val actualPartition = rv.toInt() and numPartitions - 1
        if (actualPartition == desiredPartition) {
            return Optional.of(String(keyBytes, StandardCharsets.UTF_8) + saltString)
        }
    }
    return Optional.empty()
}
