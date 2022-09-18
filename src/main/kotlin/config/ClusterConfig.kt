package config

import com.couchbase.client.core.env.NetworkResolution


class ClusterConfig(
    val hosts: List<String>,
    val username: String,
    val password: String,
    val buckets: List<CouchbaseBucket>,
    private val network: String,
) {

    fun networkResolution(): NetworkResolution {
        return when (network) {
            "auto" -> NetworkResolution.AUTO
            "external" -> NetworkResolution.EXTERNAL
            else -> NetworkResolution.DEFAULT
        }
    }

}
