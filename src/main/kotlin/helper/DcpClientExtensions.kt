package helper

import com.couchbase.client.dcp.Client
import com.couchbase.client.dcp.PasswordAuthenticator
import com.couchbase.client.dcp.StaticCredentialsProvider
import com.couchbase.client.dcp.StreamFrom
import com.couchbase.client.dcp.StreamTo
import java.time.Duration
import java.util.*

fun Client.getCurrentSeqnosAsMap(timeout: Duration): Map<Int, Long> {
    val block = this.seqnos.collectList().block(timeout)
    if (block != null) {
        return block.associate { it.partition() to it.seqno() }
    }
    return TreeMap()
}

fun initDcpClient(host: List<String>, bucketName: String, username: String, password: String): Client {
    val dcpClient = Client.builder()
        .seedNodes(host)
        .bucket(bucketName)
        .authenticator(PasswordAuthenticator(StaticCredentialsProvider(username, password)))
        .build()

    dcpClient.connect().block()
    dcpClient.initializeState(StreamFrom.NOW, StreamTo.INFINITY).block()

    return dcpClient
}
