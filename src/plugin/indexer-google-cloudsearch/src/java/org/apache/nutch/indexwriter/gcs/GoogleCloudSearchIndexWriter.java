/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.indexwriter.gcs;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Service;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.FieldOrValue;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudSearchIndexWriter implements IndexWriter {
  @VisibleForTesting
  static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String CONFIG_KEY_CONFIG_FILE = "gcs.config.file";
  public static final String CONFIG_KEY_UPLOAD_FORMAT = "gcs.uploadFormat";
  public static final String FIELD_ID = "id";
  public static final String FIELD_URL = "url";
  public static final String FIELD_RAW_CONTENT = "binaryContent";
  public static final String FIELD_TEXT_CONTENT = "content"; // provided by plugin: index-basic
  public static final String FIELD_CONTENT_TYPE = "type"; // provided by plugin: index-more

  static final String ITEM_METADATA_TITLE_DEFAULT = "title";
  static final String ITEM_METADATA_UPDATE_TIME_DEFAULT = "lastModified";

  private final Helper helper;
  private String configPath;
  private UploadFormat uploadFormat = UploadFormat.RAW;
  private org.apache.hadoop.conf.Configuration config;
  private IndexingService indexingService;
  private DefaultAcl defaultAcl;

  public enum UploadFormat {
    RAW,
    TEXT
  }

  public GoogleCloudSearchIndexWriter() {
    this(new Helper());
  }

  @VisibleForTesting
  GoogleCloudSearchIndexWriter(Helper helper) {
    this.helper = helper;
  }

  @Override
  public void open(org.apache.hadoop.conf.Configuration conf, String name) throws IOException {
    throw new UnsupportedOperationException("Unsupported deprecated method");
  }

  @Override
  public void open(IndexWriterParams parameters) throws IOException {
    LOG.info("Starting up!");
    initSDKConfig(parameters);
    updateUploadFormat(parameters);
    indexingService = createIndexingService();
    ((Service) indexingService).startAsync().awaitRunning();
    defaultAcl = helper.initDefaultAclFromConfig(indexingService);
    synchronized (this) {
      if (!StructuredData.isInitialized()) {
        StructuredData.initFromConfiguration(indexingService);
      }
    }
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    Stopwatch stopWatch = Stopwatch.createStarted();
    String id = (String) doc.getFieldValue(FIELD_ID);
    String url = (String) doc.getFieldValue(FIELD_URL);
    String contentType = (String) doc.getFieldValue(FIELD_CONTENT_TYPE);
    if (Strings.isNullOrEmpty(contentType)) {
      throw new IOException(
          "ContentType ('type') field is missing, please enable the index-more plugin!");
    }

    AbstractInputStreamContent contentStream = getInputStreamContent(doc, contentType);
    try {
      Item item = createItem(doc, contentType);
      // Try DefaultAcl, grant customer's GSuite domain if unavailable
      if (!defaultAcl.applyToIfEnabled(item)) {
        item.setAcl(
            new ItemAcl().setReaders(Collections.singletonList(Acl.getCustomerPrincipal())));
      }

      indexingService.indexItemAndContent(
          item,
          contentStream,
          null, // hash, since push queues are not used
          uploadFormat == UploadFormat.RAW ? ContentFormat.RAW : ContentFormat.TEXT,
          RequestMode.ASYNCHRONOUS);
      stopWatch.stop();
      // TODO(sfruhwald) Change this to debug, add summary info message to close()
      LOG.info(
          "Document ("
              + contentType
              + ") indexed ("
              + ((contentStream.getLength() == -1) ? "Unknown length"
                  : FileUtils.byteCountToDisplaySize(contentStream.getLength()))
              + " / "
              + stopWatch.elapsed(TimeUnit.MILLISECONDS)
              + "ms): "
              + url);
    } catch (IOException | RuntimeException e) {
      LOG.warn("Exception caught while indexing: ", e);
    }
  }

  private AbstractInputStreamContent getInputStreamContent(NutchDocument doc, String contentType)
      throws IOException {
    if (uploadFormat == UploadFormat.RAW) {
      try {
        byte[] contentBytes =
            Base64.getDecoder().decode((String) doc.getFieldValue(FIELD_RAW_CONTENT));
        return new ByteArrayContent(contentType, contentBytes);
      } catch (IllegalArgumentException | NullPointerException e) {
        throw new IOException(
            "Error: binaryContent not available or not Base64 encoded. Please add"
                + " \"-addBinaryContent -base64\" to 'nutch index' command-line arguments!",
            e);
      }
    }
    String textContent = (String) doc.getFieldValue(FIELD_TEXT_CONTENT);
    if (textContent == null) {
      throw new IOException(
          "Text content ('content') field is missing, please enable the index-basic plugin!");
    }
    return new ByteArrayContent(contentType, textContent.getBytes(StandardCharsets.UTF_8));
  }

  private Item createItem(NutchDocument doc, String contentType) throws IOException {
    Multimap<String, Object> multimap = ArrayListMultimap.create();
    for (Map.Entry<String, NutchField> entry : doc) {
      multimap.putAll(entry.getKey(), entry.getValue().getValues());
    }
    return IndexingItemBuilder.fromConfiguration((String) doc.getFieldValue(FIELD_ID))
        .setItemType(ItemType.CONTENT_ITEM)
        .setMimeType(contentType)
        .setSourceRepositoryUrl(FieldOrValue.withValue((String) doc.getFieldValue(FIELD_URL)))
        .setValues(multimap)
        .setTitle(FieldOrValue.withField(ITEM_METADATA_TITLE_DEFAULT))
        .setUpdateTime(FieldOrValue.withField(ITEM_METADATA_UPDATE_TIME_DEFAULT))
        .build();
  }

  private IndexingService createIndexingService() throws IOException {
    IndexingService indexingService;
    try {
      indexingService = helper.createIndexingService();
    } catch (GeneralSecurityException | IOException e) {
      throw new IOException("failed to create IndexingService", e);
    }
    return indexingService;
  }

  @Override
  public void delete(String key) throws IOException {
    // TODO(sfruhwald) Add debug level, per-document logging here, like in write()
    indexingService.deleteItem(key, Long.toString(helper.getCurrentTimeMillis()).getBytes(),
        RequestMode.ASYNCHRONOUS);
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void commit() throws IOException {
    // Unsupported
  }

  @Override
  public void close() {
    Stopwatch stopWatch = Stopwatch.createStarted();
    if ((indexingService != null) && indexingService.isRunning()) {
      ((Service) indexingService).stopAsync().awaitTerminated();
    }
    stopWatch.stop();
    LOG.info("Shutting down (took: " + stopWatch.elapsed(TimeUnit.MILLISECONDS) + "ms)!");
  }

  @Override
  public String describe() {
    return "Google Cloud Search Indexer";
  }

  @Override
  public void setConf(org.apache.hadoop.conf.Configuration conf) {
    config = conf;
  }

  @Override
  public org.apache.hadoop.conf.Configuration getConf() {
    return config;
  }

  private void updateUploadFormat(IndexWriterParams parameters) throws IOException {
    String uploadFormatValue = parameters.get(CONFIG_KEY_UPLOAD_FORMAT);
    if (uploadFormatValue != null) {
      try {
        uploadFormat = UploadFormat.valueOf(uploadFormatValue.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IOException("Unknown value for '" + CONFIG_KEY_UPLOAD_FORMAT + "'", e);
      }
    }
  }

  private void initSDKConfig(IndexWriterParams parameters) throws IOException {
    configPath = parameters.get(CONFIG_KEY_CONFIG_FILE);
    if (configPath == null) {
      throw new IOException("Missing required configuration parameter: " + CONFIG_KEY_CONFIG_FILE);
    }
    String[] args = {"-Dconfig=" + configPath};
    if (!helper.isConfigIinitialized()) {
      try {
        helper.initConfig(args);
      } catch (IOException e) {
        throw new IOException(
            "Failed to initialize SDK configuration. Check the configuration file and try again!",
            e);
      }
    }
  }

  static class Helper {

    boolean isConfigIinitialized() {
      return Configuration.isInitialized();
    }

    void initConfig(String[] args) throws IOException {
      Configuration.initConfig(args);
    }

    DefaultAcl initDefaultAclFromConfig(IndexingService indexingService) {
      return DefaultAcl.fromConfiguration(indexingService);
    }

    long getCurrentTimeMillis() {
      return System.currentTimeMillis();
    }

    IndexingService createIndexingService() throws IOException, GeneralSecurityException {
      return IndexingServiceImpl.Builder
          .fromConfiguration(Optional.empty() /* credential factory */, this.getClass().getName())
          .build();
    }
  }
}
