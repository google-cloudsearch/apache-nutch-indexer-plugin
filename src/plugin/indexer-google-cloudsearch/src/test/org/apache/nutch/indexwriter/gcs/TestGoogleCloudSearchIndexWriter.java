package org.apache.nutch.indexwriter.gcs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.util.DateTime;
import com.google.api.services.cloudsearch.v1.model.BooleanPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.NamedProperty;
import com.google.api.services.cloudsearch.v1.model.ObjectDefinition;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PropertyDefinition;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.StructuredDataObject;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexwriter.gcs.GoogleCloudSearchIndexWriter.Helper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestGoogleCloudSearchIndexWriter {
  private static final String CONTENT = "Test1234567890";
  private static final String CONTENT_BASE64 = "VGVzdDEyMzQ1Njc4OTA=";
  private static final String URL = "http://x.yz/abc";
  private static final String ID = "XYZ123";
  private static final String MIME_TEXT = "text/plain";
  private static final String MIME_PDF = "text/pdf";
  private static final long CURRENT_MILLIS = 123456789;
  private static final boolean APPLY_DOMAIN_ACLS = true;

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();

  @Mock private Helper mockHelper;
  @Mock private IndexWriterParams mockParams;
  @Mock private DefaultAcl mockDefaultAcl;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private IndexingService mockIndexingService;

  @Captor private ArgumentCaptor<ByteArrayContent> itemContentCaptor;
  @Captor private ArgumentCaptor<Item> itemCaptor;

  private GoogleCloudSearchIndexWriter subject;

  @Before
  public void setUp() throws Exception {
    when(mockHelper.createIndexingService()).thenReturn(mockIndexingService);
    when(mockHelper.initDefaultAclFromConfig(mockIndexingService)).thenReturn(mockDefaultAcl);
    when(mockHelper.getCurrentTimeMillis()).thenReturn(CURRENT_MILLIS);
    when(mockHelper.isConfigIinitialized()).thenReturn(true);
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_CONFIG_FILE))
        .thenReturn("/path/to/config");
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT)).thenReturn("RAW");
    when(mockDefaultAcl.applyToIfEnabled(any())).thenReturn(true);
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    subject = new GoogleCloudSearchIndexWriter(mockHelper);
  }

  @Test
  public void verifyConstantValues() {
    assertEquals(GoogleCloudSearchIndexWriter.CONFIG_KEY_CONFIG_FILE, "gcs.config.file");
    assertEquals(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT, "gcs.uploadFormat");
    assertEquals(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, "type");
    assertEquals(GoogleCloudSearchIndexWriter.FIELD_URL, "url");
    assertEquals(GoogleCloudSearchIndexWriter.FIELD_ID, "id");
    assertEquals(GoogleCloudSearchIndexWriter.FIELD_RAW_CONTENT, "binaryContent");
    assertEquals(GoogleCloudSearchIndexWriter.FIELD_TEXT_CONTENT, "content");
  }

  @Test
  public void fullLifecycle() throws IOException, GeneralSecurityException {
    setupConfig.initConfig(new Properties());
    when(mockHelper.isConfigIinitialized()).thenReturn(false);
    when(mockIndexingService.isRunning()).thenReturn(true);
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_RAW_CONTENT, CONTENT_BASE64);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_PDF);
    subject.write(doc);
    subject.update(doc);
    subject.delete(ID);
    subject.close();
    InOrder inOrder = Mockito.inOrder(mockHelper, mockIndexingService);
    inOrder.verify(mockHelper).isConfigIinitialized();
    inOrder.verify(mockHelper).initConfig(any());
    inOrder.verify(mockHelper).createIndexingService();
    inOrder.verify(mockIndexingService).startAsync();
    inOrder.verify(mockHelper).initDefaultAclFromConfig(mockIndexingService);
    inOrder
        .verify(mockIndexingService, times(2))
        .indexItemAndContent(any(), any(), any(), any(), any());
    inOrder.verify(mockIndexingService).deleteItem(any(), any(), any());
    inOrder.verify(mockIndexingService).isRunning();
    inOrder.verify(mockIndexingService).stopAsync();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void openShouldInitializeConfig() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockHelper.isConfigIinitialized()).thenReturn(false);
    subject.open(mockParams);
    verify(mockHelper, times(1)).isConfigIinitialized();
    verify(mockHelper).initConfig(new String[] {"-Dconfig=/path/to/config"});
  }

  @Test
  public void openShouldNotReinitializeConfigWhenAlreadyInitialized() throws IOException {
    setupConfig.initConfig(new Properties());
    subject.open(mockParams);
    verify(mockHelper, times(1)).isConfigIinitialized();
    verify(mockHelper, times(0)).initConfig(any());
  }

  @Test
  public void openShouldFailWhenInitializingConfigThrowsAnException() throws IOException {
    when(mockHelper.isConfigIinitialized()).thenReturn(false);
    doThrow(new IOException()).when(mockHelper).initConfig(any());
    thrown.expect(IOException.class);
    thrown.expectMessage(
        "Failed to initialize SDK configuration. Check the configuration file and try again!");
    subject.open(mockParams);
  }

  @Test
  public void openShouldFailWithmissingConfigPath() throws IOException {
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_CONFIG_FILE)).thenReturn(null);
    thrown.expect(IOException.class);
    subject.open(mockParams);
  }

  @Test
  public void openShouldCreateIndexingService() throws Exception {
    setupConfig.initConfig(new Properties());
    subject.open(mockParams);
    verify(mockHelper).createIndexingService();
  }

  @Test
  public void openShouldFailWhenCreatingIndexingServiceFailsWithSecurityException()
      throws Exception {
    when(mockHelper.createIndexingService()).thenThrow(new GeneralSecurityException());
    thrown.expect(IOException.class);
    thrown.expectMessage("failed to create IndexingService");
    subject.open(mockParams);
  }

  @Test
  public void openShouldFailWhenCreatingIndexingServiceFailsWithIOException() throws Exception {
    when(mockHelper.createIndexingService()).thenThrow(new IOException());
    thrown.expect(IOException.class);
    thrown.expectMessage("failed to create IndexingService");
    subject.open(mockParams);
  }

  @Test
  public void openShouldFailWhenUploadFormatHasInvalidValue() throws IOException {
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT))
        .thenReturn("Invalid_Value");
    thrown.expect(IOException.class);
    thrown.expectMessage(
        "Unknown value for '" + GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT + "'");
    subject.open(mockParams);
  }

  @Test
  public void openShouldStartIndexingService() throws IOException {
    setupConfig.initConfig(new Properties());
    subject.open(mockParams);
    verify(mockIndexingService.startAsync()).awaitRunning();
  }

  @Test
  public void deleteShouldSimplyDelegateTheCallToIndexingService() throws IOException {
    setupConfig.initConfig(new Properties());
    String key = URL;
    subject.open(mockParams);
    subject.delete(key);
    verify(mockIndexingService, times(1)).deleteItem(key, Long.toString(CURRENT_MILLIS).getBytes(),
        RequestMode.ASYNCHRONOUS);
  }

  @Test
  public void commitShouldDoNothingOrAtLeastDoNotInteractWithDeps() throws IOException {
    subject.commit();
    verifyNoMoreInteractions(mockParams, mockDefaultAcl, mockIndexingService);
  }

  @Test
  public void writeShouldFailWhenRawUploadModeIsSelectedAndBinaryContentIsNotInValidBase64()
      throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldFailWhenRawUploadModeIsSelectedAndBinaryContentIsNotInValidBase64(subject::write);
  }

  @Test
  public void writeShouldFailWhenRawUploadModeIsSelectedAndBinaryContentFieldIsMissing()
      throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldFailWhenRawUploadModeIsSelectedAndBinaryContentFieldIsMissing(subject::write);
  }

  @Test
  public void writeShouldFailWhenTextUploadModeIsSelectedAndTextContentFieldIsMissing()
      throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldFailWhenTextUploadModeIsSelectedAndTextContentFieldIsMissing(subject::write);
  }

  @Test
  public void writeShouldFailWhenContentTypeFieldIsMissing() throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldFailWhenContentTypeFieldIsMissing(subject::write);
  }

  @Test
  public void writeShouldNotFailWhenIndexingServiceThrowsIOException() throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldNotFailWhenIndexingServiceThrowsIOException(subject::write);
  }

  @Test
  public void writeShouldNotFailWhenAddItemThrowsRuntimeException() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT)).thenReturn("Text");
    SettableFuture<Operation> settable = SettableFuture.create();
    // It's ideal to test RuntimeException from createItem, but since we can't mock it,
    // using mockIndexingService.indexItemAndContent() to throw RuntimeException.
    when(mockIndexingService.indexItemAndContent(any(), any(), any(), any(), any()))
        .thenReturn(settable)
        .thenThrow(new RuntimeException())
        .thenReturn(settable);
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_TEXT_CONTENT, CONTENT);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_TEXT);
    subject.write(doc);
    subject.write(doc);
    subject.write(doc);
    verify(mockIndexingService, times(3)).indexItemAndContent(any(), any(), any(), any(), any());
  }

  @Test
  public void writeSuccessfulWithDefaultUploadFormatAndCustomerDomainAcls() throws IOException {
    setupConfig.initConfig(new Properties());
    successfulWithDefaultUploadFormatAndCustomerDomainAcls(subject::write);
  }

  @Test
  public void writeSuccessfulRawContent() throws IOException {
    setupConfig.initConfig(new Properties());
    successfulRawContent(subject::write);
  }

  @Test
  public void writeSuccessfulTextContent() throws IOException {
    setupConfig.initConfig(new Properties());
    successfulTextContent(subject::write);
  }

  @Test
  public void updateShouldFailWhenRawUploadModeIsSelectedAndBinaryContentIsNotInValidBase64()
      throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldFailWhenRawUploadModeIsSelectedAndBinaryContentIsNotInValidBase64(subject::update);
  }

  @Test
  public void updateShouldFailWhenRawUploadModeIsSelectedAndBinaryContentFieldIsMissing()
      throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldFailWhenRawUploadModeIsSelectedAndBinaryContentFieldIsMissing(subject::update);
  }

  @Test
  public void updateShouldFailWhenTextUploadModeIsSelectedAndTextContentFieldIsMissing()
      throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldFailWhenTextUploadModeIsSelectedAndTextContentFieldIsMissing(subject::update);
  }

  @Test
  public void updateShouldFailWhenContentTypeFieldIsMissing() throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldFailWhenContentTypeFieldIsMissing(subject::update);
  }

  @Test
  public void updateShouldNotFailWhenIndexingServiceThrowsIOException() throws IOException {
    setupConfig.initConfig(new Properties());
    subjectShouldNotFailWhenIndexingServiceThrowsIOException(subject::update);
  }

  @Test
  public void updateSuccessfulWithDefaultUploadFormatAndCustomerDomainAcls() throws IOException {
    setupConfig.initConfig(new Properties());
    successfulWithDefaultUploadFormatAndCustomerDomainAcls(subject::update);
  }

  @Test
  public void updateSuccessfulRawContent() throws IOException {
    setupConfig.initConfig(new Properties());
    successfulRawContent(subject::update);
  }

  @Test
  public void updateSuccessfulTextContent() throws IOException {
    setupConfig.initConfig(new Properties());
    successfulTextContent(subject::update);
  }

  @Test
  public void describeShouldReturnPluginName() {
    assertEquals("Google Cloud Search Indexer", subject.describe());
  }

  @Test
  public void closeShouldStopIndexingService() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.isRunning()).thenReturn(true);
    subject.open(mockParams);
    subject.close();
    verify(mockIndexingService).isRunning();
    verify(mockIndexingService.stopAsync()).awaitTerminated();
  }

  @Test
  public void successfulItemMetadataFields() throws IOException {
    // Title is not set, so the default is used. updateTime is set, but
    // the default has a non-empty value, so the default is used.
    Properties config = new Properties();
    config.put(IndexingItemBuilder.UPDATE_TIME_FIELD, "modified");
    config.put(IndexingItemBuilder.CREATE_TIME_FIELD, "created");
    config.put(IndexingItemBuilder.CONTENT_LANGUAGE_VALUE, "en");
    setupConfig.initConfig(config);

    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT)).thenReturn("TEXT");
    subject.open(mockParams);

    String title = "Helo";
    String language = "en";
    String created = new DateTime("2018-08-10T10:10:10.100Z").toString();
    String modified = new DateTime("2018-08-13T15:01:23.100Z").toString();
    String lastModified = new DateTime("2018-08-18T18:01:23.100Z").toString();
    String content = "<html> <head> <title> Hello </title>"
        + "</head> <body> Hello </body></html>";

    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_TEXT_CONTENT, content);
    doc.add("title", title);
    doc.add("created", created);
    doc.add("modified", modified);
    doc.add("lastModified", lastModified);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_TEXT);
    subject.write(doc);

    verify(mockIndexingService)
      .indexItemAndContent(
          itemCaptor.capture(),
          any(),
          any(),
          any(),
          any());

    ItemMetadata expectedItemMetadata =
        new ItemMetadata()
          .setTitle(title)
          .setMimeType(MIME_TEXT)
          .setSourceRepositoryUrl(URL)
          .setContentLanguage(language)
          .setCreateTime(created)
          .setUpdateTime(lastModified);
    assertEquals(expectedItemMetadata, itemCaptor.getValue().getMetadata());
  }

  @Test
  public void successfulItemStructuredData() throws IOException {
    PropertyDefinition prop1 = new PropertyDefinition().setName("approved").setIsRepeatable(false)
        .setIsReturnable(true).setBooleanPropertyOptions(new BooleanPropertyOptions());
    Schema schema = new Schema();
    schema.setObjectDefinitions(
        Arrays.asList(
            new ObjectDefinition()
                .setName("schema1")
                .setPropertyDefinitions(Arrays.asList(prop1))));

    Properties config = new Properties();
    config.put(IndexingItemBuilder.OBJECT_TYPE, "schema1");
    setupConfig.initConfig(config);

    when(mockIndexingService.getSchema()).thenReturn(schema);
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT)).thenReturn("TEXT");
    subject.open(mockParams);

    String content = "<html> <head> <title> Hello </title>"
        + "<meta name='approved' content='true' /></head> <body> Hello </body></html>";
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_TEXT_CONTENT, content);
    doc.add("approved", "true");
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_TEXT);
    subject.write(doc);

    verify(mockIndexingService)
      .indexItemAndContent(
          itemCaptor.capture(),
          any(),
          any(),
          any(),
          any());

    NamedProperty approved = new NamedProperty().setName("approved").setBooleanValue(true);
    StructuredDataObject structuredData =
        new StructuredDataObject().setProperties(Arrays.asList(approved));
    assertEquals(structuredData, itemCaptor.getValue().getStructuredData().getObject());
  }

  //TODO (sveldurthi): Add test for multi-value field.

  private Item goldenItem(boolean applyDomainAcl, String mimeType) {
    Item item =
        new Item()
            .setName(ID)
            .setItemType(ItemType.CONTENT_ITEM.name())
            .setMetadata(new ItemMetadata().setSourceRepositoryUrl(URL).setMimeType(mimeType));
    if (applyDomainAcl) {
      item.setAcl(new ItemAcl().setReaders(Collections.singletonList(Acl.getCustomerPrincipal())));
    }
    return item;
  }

  private void subjectShouldFailWhenRawUploadModeIsSelectedAndBinaryContentIsNotInValidBase64(
      ThrowingConsumer<NutchDocument, IOException> subjectFunction) throws IOException {
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_PDF);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_RAW_CONTENT, "Content_not_in+Base64");
    thrown.expect(IOException.class);
    thrown.expectMessage(
        "Error: binaryContent not available or not Base64 encoded. Please add"
            + " \"-addBinaryContent -base64\" to 'nutch index' command-line arguments!");
    subjectFunction.accept(doc);
  }

  private void subjectShouldFailWhenRawUploadModeIsSelectedAndBinaryContentFieldIsMissing(
      ThrowingConsumer<NutchDocument, IOException> subjectFunction) throws IOException {
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_PDF);
    thrown.expect(IOException.class);
    thrown.expectMessage(
        "Error: binaryContent not available or not Base64 encoded. Please add"
            + " \"-addBinaryContent -base64\" to 'nutch index' command-line arguments!");
    subjectFunction.accept(doc);
  }

  private void subjectShouldFailWhenTextUploadModeIsSelectedAndTextContentFieldIsMissing(
      ThrowingConsumer<NutchDocument, IOException> subjectFunction) throws IOException {
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT)).thenReturn("Text");
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_TEXT);
    thrown.expect(IOException.class);
    thrown.expectMessage(
        "Text content ('content') field is missing, please enable the index-basic plugin!");
    subjectFunction.accept(doc);
  }

  private void subjectShouldFailWhenContentTypeFieldIsMissing(
      ThrowingConsumer<NutchDocument, IOException> subjectFunction) throws IOException {
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_RAW_CONTENT, CONTENT_BASE64);
    thrown.expect(IOException.class);
    thrown.expectMessage(
        "ContentType ('type') field is missing, please enable the index-more plugin!");
    subjectFunction.accept(doc);
  }

  private void subjectShouldNotFailWhenIndexingServiceThrowsIOException(
      ThrowingConsumer<NutchDocument, IOException> subjectFunction) throws IOException {
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT)).thenReturn("Text");
    when(mockIndexingService.indexItemAndContent(any(), any(), any(), any(), any()))
        .thenThrow(new IOException());
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_TEXT_CONTENT, CONTENT);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_TEXT);
    try {
      subjectFunction.accept(doc);
    } catch (IOException e) {
      fail();
    }
    verify(mockIndexingService).indexItemAndContent(any(), any(), any(), any(), any());
  }

  private void successfulWithDefaultUploadFormatAndCustomerDomainAcls(
      ThrowingConsumer<NutchDocument, IOException> subjectFunction) throws IOException {
    when(mockDefaultAcl.applyToIfEnabled(any())).thenReturn(false);
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT)).thenReturn(null);
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_RAW_CONTENT, CONTENT_BASE64);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_PDF);
    subjectFunction.accept(doc);
    verify(mockIndexingService)
        .indexItemAndContent(
            eq(goldenItem(APPLY_DOMAIN_ACLS, MIME_PDF)),
            itemContentCaptor.capture(),
            eq(null),
            eq(ContentFormat.RAW),
            eq(RequestMode.ASYNCHRONOUS));
    assertEquals(itemContentCaptor.getValue().getType(), MIME_PDF);
    assertTrue(
        Arrays.equals(
            IOUtils.toByteArray(itemContentCaptor.getValue().getInputStream()),
            CONTENT.getBytes()));
  }

  private void successfulRawContent(ThrowingConsumer<NutchDocument, IOException> subjectFunction)
      throws IOException {
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_RAW_CONTENT, CONTENT_BASE64);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_PDF);
    subjectFunction.accept(doc);
    verify(mockIndexingService)
        .indexItemAndContent(
            eq(goldenItem(!APPLY_DOMAIN_ACLS, MIME_PDF)),
            itemContentCaptor.capture(),
            eq(null),
            eq(ContentFormat.RAW),
            eq(RequestMode.ASYNCHRONOUS));
    assertEquals(itemContentCaptor.getValue().getType(), MIME_PDF);
    assertTrue(
        Arrays.equals(
            IOUtils.toByteArray(itemContentCaptor.getValue().getInputStream()),
            CONTENT.getBytes()));
  }

  private void successfulTextContent(ThrowingConsumer<NutchDocument, IOException> subjectFunction)
      throws IOException {
    when(mockParams.get(GoogleCloudSearchIndexWriter.CONFIG_KEY_UPLOAD_FORMAT)).thenReturn("TEXT");
    subject.open(mockParams);
    NutchDocument doc = new NutchDocument();
    doc.add(GoogleCloudSearchIndexWriter.FIELD_ID, ID);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_URL, URL);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_TEXT_CONTENT, CONTENT);
    doc.add(GoogleCloudSearchIndexWriter.FIELD_CONTENT_TYPE, MIME_TEXT);
    subjectFunction.accept(doc);
    verify(mockIndexingService)
        .indexItemAndContent(
            eq(goldenItem(!APPLY_DOMAIN_ACLS, MIME_TEXT)),
            itemContentCaptor.capture(),
            eq(null),
            eq(ContentFormat.TEXT),
            eq(RequestMode.ASYNCHRONOUS));
    assertEquals(itemContentCaptor.getValue().getType(), MIME_TEXT);
    assertTrue(
        Arrays.equals(
            IOUtils.toByteArray(itemContentCaptor.getValue().getInputStream()),
            CONTENT.getBytes()));
  }

  @FunctionalInterface
  public interface ThrowingConsumer<T, E extends Exception> {
    void accept(T t) throws E;
  }
}
