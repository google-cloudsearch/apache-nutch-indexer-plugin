# Google Cloud Search Apache Nutch Indexer Plugin

The Google Cloud Search Apache Nutch indexer plugin extends existing
[Apache Nutch](http://nutch.apache.org/) installation to crawl and index content to Google Cloud
Search with support for ACLs and metadata.

This connector is an implementation of the
[Apache Nutch Indexer Plugin API](https://wiki.apache.org/nutch/IndexWriters).

## Build instructions

1. Build the connector

   a. Clone the connector repository from GitHub:
      ```
      git clone https://github.com/google-cloudsearch/apache-nutch-indexer-plugin.git
      cd apache-nutch-indexer-plugin
      ```

   b. Checkout the desired version of the connector and build the ZIP file:
      ```
      git checkout tags/v1-0.0.4
      mvn package
      ```
      (To skip the tests when building the connector, use `mvn package -DskipTests`)

2. Download [Apache Nutch 1.15](http://archive.apache.org/dist/nutch/1.15) and follow the Apache
   Nutch instructions (https://wiki.apache.org/nutch/NutchTutorial) to install.

3. Extract `target/google-cloudsearch-apache-nutch-indexer-plugin-v1.0.0.4.zip` built from step 2 to
   a folder. Copy `plugins/indexer-google-cloud-search` folder to the Apache Nutch install plugins
   folder (`apache-nutch-1.15/plugins`).

For further information on configuration and deployment of this indexer plugin, see
[Deploy an Apache Nutch Indexer
Plugin](https://developers.google.com/cloud-search/docs/guides/apache-nutch-connector).
