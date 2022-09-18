package service

import com.slack.api.Slack
import com.slack.api.methods.MethodsClient
import com.slack.api.methods.request.chat.ChatPostMessageRequest
import config.SlackConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SlackClient(private val slackConfig: SlackConfig) {
    private val slack: Slack = Slack.getInstance()
    private val methods: MethodsClient = slack.methods(slackConfig.token)

    fun sendNotification(msg: String) {
        val request = ChatPostMessageRequest.builder()
            .channel(slackConfig.channel)
            .iconEmoji(slackConfig.iconEmoji)
            .username(slackConfig.username)
            .text(msg)
            .build()

        val chatPostMessage = methods.chatPostMessage(request)

        if (!chatPostMessage.isOk) {
            logger.error("Error while sending slack notification, err: " + chatPostMessage.error)
        }

    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlackClient::class.java)
    }
}
