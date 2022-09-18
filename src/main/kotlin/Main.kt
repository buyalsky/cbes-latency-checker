import com.couchbase.client.dcp.Client
import com.moandjiezana.toml.Toml
import config.CouchbaseConfig
import config.SlackConfig
import helper.CouchbaseHelper
import helper.initDcpClient
import org.slf4j.LoggerFactory
import repository.CouchbaseCheckpointRepository
import service.OffsetChecker
import service.SlackClient
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.SynchronousQueue


class Main

fun main() {
    val synchronousQueue = SynchronousQueue<Throwable>()
    val logger = LoggerFactory.getLogger(Main::class.java)
    logger.info("STARTING CBES latency checker")

    val couchbaseConfigPath: String = Main::class.java.getResource("couchbase.toml").path
    val couchbaseConfigFile = File(couchbaseConfigPath)
    val couchbaseConfigToml: Toml = Toml().read(couchbaseConfigFile)
    val couchbaseConfig = couchbaseConfigToml.to(CouchbaseConfig::class.java)

    val slackConfigPath: String = Main::class.java.getResource("slack.toml").path
    val slackConfigFile = File(slackConfigPath)
    val slackConfigToml: Toml = Toml().read(slackConfigFile)
    val slackConfig = slackConfigToml.to(SlackConfig::class.java)

    val slackClient = SlackClient(slackConfig)

    val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    val offsetCheckers = ArrayList<OffsetChecker>()

    for (clusterConfig in couchbaseConfig.clusters) {
        val cluster = CouchbaseHelper.createCluster(
            clusterConfig.hosts, clusterConfig.username, clusterConfig.password, clusterConfig.networkResolution()
        )

        for (couchbaseBucketConfig in clusterConfig.buckets) {
            val dcpClient: Client

            try {
                dcpClient = initDcpClient(
                    clusterConfig.hosts, couchbaseBucketConfig.bucket, clusterConfig.username, clusterConfig.password
                )
            } catch (e: Exception) {
                logger.error("Could not initialize DCP client for host: ${clusterConfig.hosts}, bucketName: $clusterConfig")
                continue
            }

            val checkpointBucket = CouchbaseHelper.waitForBucket(cluster, couchbaseBucketConfig.checkpointBucket())

            if (checkpointBucket == null) {
                logger.error("Could not get checkpoint bucket: ${couchbaseBucketConfig.checkpointBucket()}")
            } else {
                val checkpointCollection = checkpointBucket.defaultCollection()

                val couchbaseCheckpointRepository =
                    CouchbaseCheckpointRepository(checkpointCollection, couchbaseBucketConfig.groupName, dcpClient.numPartitions())

                val offsetChecker = OffsetChecker(
                    couchbaseCheckpointRepository,
                    dcpClient,
                    scheduledExecutorService,
                    slackClient,
                    couchbaseBucketConfig,
                )

                offsetCheckers.add(offsetChecker)
            }

        }

    }

    Runtime.getRuntime().addShutdownHook(Thread {
        offsetCheckers.forEach { it.close() }
    })

    logger.info("Starting offset checker tasks")

    offsetCheckers.forEach { it.initTask() }

    synchronousQueue.take()

}
