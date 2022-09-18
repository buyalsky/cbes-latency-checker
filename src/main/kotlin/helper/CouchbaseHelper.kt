package helper

import com.couchbase.client.core.env.IoConfig.networkResolution
import com.couchbase.client.core.env.NetworkResolution
import com.couchbase.client.core.env.PasswordAuthenticator
import com.couchbase.client.core.env.TimeoutConfig.connectTimeout
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.dcp.core.utils.CbCollections
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.Cluster
import com.couchbase.client.java.ClusterOptions
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions
import com.couchbase.client.java.env.ClusterEnvironment
import java.security.KeyStore
import java.time.Duration
import java.util.function.Supplier

object CouchbaseHelper {
    fun createCluster(hosts: List<String>, username: String, password: String, networkResolution: NetworkResolution): Cluster {
        val env = environmentBuilder(networkResolution, null).build()
        val connectionString = hosts.joinToString(separator = ",")
        val authenticator = PasswordAuthenticator.create(username, password)

        return Cluster.connect(connectionString, ClusterOptions.clusterOptions(authenticator).environment(env))
    }

    private fun environmentBuilder(networkResolution: NetworkResolution, keystore: Supplier<KeyStore>?): ClusterEnvironment.Builder {
        return ClusterEnvironment.builder()
            .ioConfig(networkResolution(networkResolution))
            .timeoutConfig(connectTimeout(Duration.ofSeconds(10)))
    }

    fun waitForBucket(cluster: Cluster, bucketName: String?): Bucket? {
        val bucket = cluster.bucket(bucketName)
        var timeout = bucket.environment().timeoutConfig().connectTimeout()

        // Multiplying timeout by 2 as a temporary workaround for JVMCBC-817
        // (giving the config loader time for the KV attempt to timeout and still leaving
        // time for loading the config from the manager).
        timeout = timeout.multipliedBy(2)
        bucket.waitUntilReady(
            timeout, WaitUntilReadyOptions.waitUntilReadyOptions().serviceTypes(
                CbCollections.setOf(
                    ServiceType.KV
                )
            )
        )
        return bucket
    }

}
