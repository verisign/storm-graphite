# 0.1.5 (unreleased)

* TBD


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
