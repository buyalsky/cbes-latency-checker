# cbes-latency-checker

CBES (Couchbase Elasticsearch Connector) latency checker keeps track of whether your connector works properly. In case there is a latency, it sends notification through slack.

It basically retrieves both the latest sequence number emitted from the replicated bucket and committed sequence number from checkpoint documents which is managed by CBES. 

## Prerequisites
* JDK 18 or above

* Make sure you've created a slack bot and given required authorizations for posting messages to your workspace. Check [Slack API page](https://api.slack.com/) for more info.

## Setup

First Edit `couchbase.toml` located in resources folder.
```toml
[[clusters]]
hosts = ['locahost']
network = 'auto'
username = 'a-user'
password = '1234'


[[clusters.buckets]]
# source bucket that CBES replicates from
bucket = 'travel-sample'

# If your connector stores replication checkpoint documents in 
# in the source bucket then you do not need to specify it. 
checkpointBucket = ''

# Group name of the connector
groupName = 'example-group'

# Threshold value for alert
threshold = 100_000

# Initial wait time before starting offset check
offsetCheckInitialDelayInSeconds = 20

# Interval time between two offset checks
offsetCheckDelayInSeconds = 10

# You can specify multiple buckets like below
[[clusters.buckets]]
bucket = 'travel-sample2'
...

# You can specify multiple clusters like below
[[clusters]]
...

[[clusters.buckets]]
...
```

Then edit `slack.toml` located in resources folder.

```toml
# Slack bot token (usually starts with xoxb-)
token=""

# Channel id that your bot send messages to
channel=""

# Icon emoji for your bot
iconEmoji=":smile:"

# Bot's user name
username="CBES latency checker"
```
