package service

import collection.RingBuffer
import collection.hasIncreasingTrajectory
import collection.hasNotChanged
import com.couchbase.client.dcp.Client
import config.CouchbaseBucket
import helper.getCurrentSeqnosAsMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import repository.CouchbaseCheckpointRepository
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit


class OffsetChecker(
    private val checkpointDao: CouchbaseCheckpointRepository,
    private val dcpClient: Client,
    private val scheduledExecutorService: ScheduledExecutorService,
    private val slackClient: SlackClient,
    couchbaseBucketConfig: CouchbaseBucket
) {
    private val latestOffsetRecords = RingBuffer<Long>(5)
    private val latestCheckpointSeqNoRecords = RingBuffer<Long>(5)
    private val latestBucketSeqNoRecords = RingBuffer<Long>(5)
    private val threshold = couchbaseBucketConfig.threshold
    private val initialDelay = couchbaseBucketConfig.offsetCheckInitialDelayInSeconds
    private val delay = couchbaseBucketConfig.offsetCheckDelayInSeconds
    private val bucket = couchbaseBucketConfig.bucket
    private val checkpointBucket = couchbaseBucketConfig.checkpointBucket()

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(OffsetChecker::class.java)
    }


    fun initTask() {
        scheduledExecutorService.scheduleWithFixedDelay(
            { measureOffset() }, initialDelay, delay, TimeUnit.SECONDS
        )
    }

    private fun measureOffset() {
        val checkpointMap = checkpointDao.loadCheckpointDocuments()

        val numPartitions = checkpointDao.getNumPartitions()
        if (checkpointMap.size != numPartitions) {
            logger.warn("Could not get entire checkpoint documents, offset check will not be performed, checkpointBucket: $checkpointBucket, groupName: ${checkpointDao.getGroupName()}")
            return
        }

        val currentSequenceNumberMap: Map<Int, Long> = dcpClient.getCurrentSeqnosAsMap(Duration.ofSeconds(5))

        if (currentSequenceNumberMap.size != numPartitions) {
            logger.warn("Could not get entire sequence numbers for bucket: $bucket, offset check will not be performed")
            return
        }

        val checkpointSeqNo = checkpointMap.values.sum()
        val actualSeqNo = currentSequenceNumberMap.values.sum()

        logger.info("Actual sequence number for bucket: $bucket is $actualSeqNo")
        logger.info("Total sequence number committed for bucket: $bucket is $checkpointSeqNo")

        val latestOffset = actualSeqNo - checkpointSeqNo

        logger.info("Offset for bucket: $bucket is $latestOffset")

        latestOffsetRecords.add(latestOffset)
        latestCheckpointSeqNoRecords.add(checkpointSeqNo)

        checkOffset(latestOffset)
    }

    private fun checkOffset(latestOffset: Long) {
        if (latestOffset > this.threshold) {
            slackClient.sendNotification("Bucket sequence number offset is more than desired threshold! Bucket: $bucket, offset: $latestOffset")
        } else if (connectorStopped()) {
            slackClient.sendNotification("Connector is not processing events! Bucket: $bucket, offset: $latestOffset")
        } else if (connectorSlow()) {
            slackClient.sendNotification("Connector is not processing quickly enough! Bucket: $bucket, offset: $latestOffset")
        }
    }

    private fun connectorSlow(): Boolean {
        return latestOffsetRecords.hasIncreasingTrajectory(this.threshold / 10)
    }

    private fun connectorStopped(): Boolean {
        return latestCheckpointSeqNoRecords.hasNotChanged() and (latestCheckpointSeqNoRecords.lastElement() < latestBucketSeqNoRecords.firstElement())
    }

    fun close() {
        this.dcpClient.close()
    }
}

