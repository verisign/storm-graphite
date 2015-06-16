# 0.2.1 (June 16, 2015)

BUG FIXES

* Fixed bug where metric data point names were not being reported in output metric path.

# 0.2.0 (June 10, 2015)

IMPROVEMENTS

* Added support for reporting to Kafka cluster.
* Added support for integrating with Confluent's [Schema Registry](https://github.com/confluentinc/schema-registry) when reporting to Kafka.


# 0.1.5 (March 25, 2015)

BREAKING CHANGES

* The metrics path of generated metrics has changed.  We are now removing the nonce from topology ids, and append the
  worker hostname, worker port, and task id to the metrics path.  This results in finer granularity of metrics and
  makes the carbon configuration of Graphite as well as querying Graphite slightly simpler.

IMPROVEMENTS

* Add configuration option `metrics.graphite.min-connect-attempt-interval-secs`, which configures the minimum wait time
  (in seconds) in between connection attempts to Graphite.
* Remove deployment specific nonce from Storm topology identifier to prevent Graphite server from building a new whisper
  database whenever a topology is redeployed.
* Add .deb packaging support.


# 0.1.4 (March 04, 2015)

IMPROVEMENTS

* Follow Fedora packaging guidelines for the RPMs we generate.


# 0.1.3 (March 03, 2015)

BUG FIXES

* (Temporarily) exclude metrics of Storm's Netty messaging layer, which were introduced in Storm 0.10.
  When running storm-graphite <= 0.1.2 on a Storm 0.10 cluster, these new metrics would result in
  NullPointerExceptions being thrown.
  See [GH-2](https://github.com/verisign/storm-graphite/issues/2) for details.
* Prevent NullPointerException when socket connection to Graphite endpoint is lost.


# 0.1.2 (January 30, 2015)

* Initial release.
