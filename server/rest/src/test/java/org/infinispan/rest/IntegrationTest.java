package org.infinispan.rest;

import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.CacheControl;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingAdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.AbstractDelegatingEmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.MIMECacheEntry;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * This tests using the Apache HTTP commons client library - but you could use anything Decided to do this instead of
 * testing the Server implementation itself, as testing the impl directly was kind of too easy. (Given that RESTEasy
 * does most of the heavy lifting !).
 *
 * @author Michael Neale
 * @author Galder Zamarreño
 * @author Michal Linhard
 * @since 4.0
 */
@Test(groups = "functional", testName = "rest.IntegrationTest")
public class IntegrationTest extends RestServerTestBase {

   private static final String HOST = "http://localhost:8888";
   private static final String cacheName = BasicCacheContainer.DEFAULT_CACHE_NAME;
   private static final String fullPath = HOST + "/rest/" + cacheName;
   private static final String DATE_PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz";
   private EmbeddedCacheManager cacheManager = null;

   private String fullPathWithPort(Method m, int port) {
      return "http://localhost:" + port + "/rest/" + cacheName + "/" + m.getName();
   }

   @BeforeClass(alwaysRun = true)
   void setUp() throws Exception {
      cacheManager = TestCacheManagerFactory.fromXml("test-config.xml");
      addServer("single", 8888, cacheManager);
      startServers();
      createClient();
   }

   EmbeddedCacheManager createCacheManager() throws IOException {
      return TestCacheManagerFactory.fromXml("test-config.xml");
   }

   @AfterClass(alwaysRun = true)
   void tearDown() throws Exception {
      destroyClient();
      stopServers();
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testBasicOperation(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();

      PutMethod insert = new PutMethod(fullPathKey);
      String initialXML = "<hey>ho</hey>";

      insert.setRequestEntity(new ByteArrayRequestEntity(initialXML
            .getBytes(), "application/octet-stream"));

      call(insert);

      assertEquals("", insert.getResponseBodyAsString().trim());
      assertEquals(HttpServletResponse.SC_OK, insert.getStatusCode());

      GetMethod get = new GetMethod(fullPathKey);
      call(get);
      byte[] bytes = get.getResponseBody();
      assertEquals(bytes.length, initialXML.getBytes().length);
      assertEquals(initialXML, get.getResponseBodyAsString());
      Header hdr = get.getResponseHeader("Content-Type");
      assertEquals("application/octet-stream", hdr.getValue());

      DeleteMethod remove = new DeleteMethod(fullPathKey);
      call(remove);
      call(get);

      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode());

      call(insert);
      call(get);
      assertEquals(initialXML, get.getResponseBodyAsString());

      DeleteMethod removeAll = new DeleteMethod(fullPath);
      assertEquals(HttpServletResponse.SC_OK, call(removeAll).getStatusCode());

      call(get);
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode());

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream oo = new ObjectOutputStream(bout);
      oo.writeObject(new MIMECacheEntry("foo", "hey".getBytes()));
      oo.flush();
      byte[] byteData = bout.toByteArray();

      PutMethod insertMore = new PutMethod(fullPathKey);

      insertMore.setRequestEntity(new ByteArrayRequestEntity(byteData, "application/octet-stream"));

      call(insertMore);

      GetMethod getMore = new GetMethod(fullPathKey);
      call(getMore);

      byte[] bytesBack = getMore.getResponseBody();
      assertEquals(byteData.length, bytesBack.length);

      ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytesBack));
      MIMECacheEntry ce = (MIMECacheEntry) oin.readObject();
      assertEquals("foo", ce.contentType);
   }

   public void testEmptyGet() throws Exception {
      assertEquals(
            HttpServletResponse.SC_NOT_FOUND,
            call(new GetMethod(HOST + "/rest/" + cacheName + "/nodata")).getStatusCode()
      );
   }

   public void testDeleteNonExistent() throws Exception {
      assertEquals(
            HttpServletResponse.SC_NOT_FOUND,
            call(new DeleteMethod(HOST + "/rest/" + cacheName + "/nodata")).getStatusCode()
      );
   }

   public void testGetCollection() throws Exception {
      PostMethod post_a = new PostMethod(fullPath + "/a");
      post_a.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(post_a);
      PostMethod post_b = new PostMethod(fullPath + "/b");
      post_b.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(post_b);

      String html = getCollection("text/html");
      assertTrue(html.contains("<a href=\"" + cacheName + "/a\">a</a>"));
      assertTrue(html.contains("<a href=\"" + cacheName + "/b\">b</a>"));

      String xml = getCollection("application/xml");
      assertTrue(xml.contains("<key>a</key>"));
      assertTrue(xml.contains("<key>b</key>"));

      String plain = getCollection("text/plain;charset=UTF-8");
      assertTrue(plain.contains("a" + System.lineSeparator()));
      assertTrue(plain.contains("b" + System.lineSeparator()));

      String json = getCollection("application/json");
      assertTrue(json.contains("\"a\""));
      assertTrue(json.contains("\"b\""));
   }

   public void testGetCollectionEscape() throws Exception {
      PostMethod post_a = new PostMethod(fullPath + "/%22a%22");
      post_a.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(post_a);
      PostMethod post_b = new PostMethod(fullPath + "/b%3E");
      post_b.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(post_b);

      String html = getCollection("text/html");
      assertTrue(html.contains("<a href=\"" + cacheName + "/&quot;a&quot;\">&quot;a&quot;</a>"));
      assertTrue(html.contains("<a href=\"" + cacheName + "/b&gt;\">b&gt;</a>"));

      String xml = getCollection("application/xml");
      assertTrue(xml.contains("<key>&quot;a&quot;</key>"));
      assertTrue(xml.contains("<key>b&gt;</key>"));

      String plain = getCollection("text/plain;charset=UTF-8");
      assertTrue(plain.contains("\"a\"" + System.lineSeparator()));
      assertTrue(plain.contains("b>" + System.lineSeparator()));

      String json = getCollection("application/json");
      assertTrue(json.contains("\\\"a\\\""));
      assertTrue(json.contains("\"b>\""));
   }

   private String getCollection(String variant) throws Exception {
      GetMethod get = new GetMethod(fullPath);
      get.addRequestHeader("Accept", variant);
      HttpMethodBase coll = call(get);
      assertEquals(HttpServletResponse.SC_OK, coll.getStatusCode());
      assertEquals(variant, coll.getResponseHeader("Content-Type").getValue());
      return coll.getResponseBodyAsString();
   }

   public void testGet(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PostMethod post = new PostMethod(fullPathKey);
      post.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(post);

      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertNotNull(get.getResponseHeader("ETag").getValue());
      assertNotNull(get.getResponseHeader("Last-Modified").getValue());
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue());
      assertEquals("data", get.getResponseBodyAsString());
   }

   public void testHead(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PostMethod post = new PostMethod(fullPathKey);
      post.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(post);

      HttpMethodBase get = call(new HeadMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertNotNull(get.getResponseHeader("ETag").getValue());
      assertNotNull(get.getResponseHeader("Last-Modified").getValue());
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue());

      assertNull(get.getResponseBodyAsString());
   }

   public void testGetIfUnmodified(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PostMethod post = new PostMethod(fullPathKey);
      post.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(post);

      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertNotNull(get.getResponseHeader("ETag").getValue());
      String lastMod = get.getResponseHeader("Last-Modified").getValue();
      assertNotNull(lastMod);
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue());
      assertEquals("data", get.getResponseBodyAsString());

      // now get again
      GetMethod getAgain = new GetMethod(fullPathKey);
      getAgain.addRequestHeader("If-Unmodified-Since", lastMod);
      get = call(getAgain);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertNotNull(get.getResponseHeader("ETag").getValue());
      assertNotNull(get.getResponseHeader("Last-Modified").getValue());
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue());
      assertEquals("data", get.getResponseBodyAsString());
   }

   public void testPostDuplicate(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PostMethod post = new PostMethod(fullPathKey);
      post.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(post);

      //Should get a conflict as its a DUPE post
      assertEquals(HttpServletResponse.SC_CONFLICT, call(post).getStatusCode());

      PutMethod put = new PutMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));

      //Should be all ok as its a put
      assertEquals(HttpServletResponse.SC_OK, call(put).getStatusCode());
   }

   public void testPutDataWithTimeToLive(Method m) throws Exception {
      putAndAssertEphemeralData(m, "2", "3");
   }

   public void testPutDataWithMaxIdleOnly(Method m) throws Exception {
      putAndAssertEphemeralData(m, "", "3");
   }

   public void testPutDataWithTimeToLiveOnly(Method m) throws Exception {
      putAndAssertEphemeralData(m, "3", "");
   }

   private void putAndAssertEphemeralData(Method m, String timeToLiveSeconds, String maxIdleTimeSeconds) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PostMethod post = new PostMethod(fullPathKey);
      int maxWaitTime = 0;
      if (!timeToLiveSeconds.isEmpty()) {
         maxWaitTime = Math.max(maxWaitTime, Integer.parseInt(timeToLiveSeconds));
         post.setRequestHeader("timeToLiveSeconds", timeToLiveSeconds);
      }

      if (!maxIdleTimeSeconds.isEmpty()) {
         maxWaitTime = Math.max(maxWaitTime, Integer.parseInt(maxIdleTimeSeconds));
         post.setRequestHeader("maxIdleTimeSeconds", maxIdleTimeSeconds);
      }

      post.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(post);

      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals("data", get.getResponseBodyAsString());

      TestingUtil.sleepThread((maxWaitTime + 1) * 1000);
      call(get);
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode());
   }

   public void testPutDataWithIfMatch(Method m) throws Exception {
      // Put the data first
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      // Now get it to retrieve some attributes
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      String etag = get.getResponseHeader("ETag").getValue();

      // Put again using the If-Match with the ETag we got back from the get
      PutMethod reput = new PutMethod(fullPathKey);
      reput.setRequestHeader("If-Match", etag);
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      assertEquals(HttpServletResponse.SC_OK, call(reput).getStatusCode());

      // Try to put again, but with a different ETag
      PutMethod reputAgain = new PutMethod(fullPathKey);
      reputAgain.setRequestHeader("If-Match", "x");
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(reputAgain).getStatusCode());
   }

   public void testPutDataWithIfNoneMatch(Method m) throws Exception {
      // Put the data first
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      // Now get it to retrieve some attributes
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      String etag = get.getResponseHeader("ETag").getValue();

      // Put again using the If-Match with the ETag we got back from the get
      PutMethod reput = new PutMethod(fullPathKey);
      reput.setRequestHeader("If-None-Match", "x");
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      assertEquals(HttpServletResponse.SC_OK, call(reput).getStatusCode());

      // Try to put again, but with a different ETag
      PutMethod reputAgain = new PutMethod(fullPathKey);
      reputAgain.setRequestHeader("If-None-Match", etag);
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(reputAgain).getStatusCode());
   }

   public void testPutDataWithIfModifiedSince(Method m) throws Exception {
      // Put the data first
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      // Now get it to retrieve some attributes
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      String lastMod = get.getResponseHeader("Last-Modified").getValue();

      // Put again using the If-Modified-Since with the lastMod we got back from the get
      PutMethod reput = new PutMethod(fullPathKey);
      reput.setRequestHeader("If-Modified-Since", lastMod);
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      assertEquals(HttpServletResponse.SC_NOT_MODIFIED, call(reput).getStatusCode());

      // Try to put again, but with an older last modification date
      PutMethod reputAgain = new PutMethod(fullPathKey);
      String dateMinus = addDay(lastMod, -1);
      reputAgain.setRequestHeader("If-Modified-Since", dateMinus);
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      assertEquals(HttpServletResponse.SC_OK, call(reputAgain).getStatusCode());
   }

   public void testPutDataWithIfUnModifiedSince(Method m) throws Exception {
      // Put the data first
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      // Now get it to retrieve some attributes
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      String lastMod = get.getResponseHeader("Last-Modified").getValue();

      // Put again using the If-Unmodified-Since with a date earlier than the one we got back from the GET
      PutMethod reput = new PutMethod(fullPathKey);
      String dateMinus = addDay(lastMod, -1);
      reput.setRequestHeader("If-Unmodified-Since", dateMinus);
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(reput).getStatusCode());

      // Try to put again, but using the date returned by the GET
      PutMethod reputAgain = new PutMethod(fullPathKey);
      reputAgain.setRequestHeader("If-Unmodified-Since", lastMod);
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      assertEquals(HttpServletResponse.SC_OK, call(reputAgain).getStatusCode());
   }

   public void testDeleteDataWithIfMatch(Method m) throws Exception {
      // Put the data first
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      // Now get it to retrieve some attributes
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      String etag = get.getResponseHeader("ETag").getValue();

      // Attempt to delete with a wrong ETag
      DeleteMethod delete = new DeleteMethod(fullPathKey);
      delete.setRequestHeader("If-Match", "x");
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(delete).getStatusCode());

      // Try to delete again, but with the proper ETag
      DeleteMethod deleteAgain = new DeleteMethod(fullPathKey);
      deleteAgain.setRequestHeader("If-Match", etag);
      assertEquals(HttpServletResponse.SC_OK, call(deleteAgain).getStatusCode());
   }

   public void testDeleteDataWithIfNoneMatch(Method m) throws Exception {
      // Put the data first
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      // Now get it to retrieve some attributes
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      String etag = get.getResponseHeader("ETag").getValue();

      // Attempt to delete with the ETag
      DeleteMethod delete = new DeleteMethod(fullPathKey);
      delete.setRequestHeader("If-None-Match", etag);
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(delete).getStatusCode());

      // Try to delete again, but with a non-matching ETag
      DeleteMethod deleteAgain = new DeleteMethod(fullPathKey);
      deleteAgain.setRequestHeader("If-None-Match", "x");
      assertEquals(HttpServletResponse.SC_OK, call(deleteAgain).getStatusCode());
   }

   public void testDeleteDataWithIfModifiedSince(Method m) throws Exception {
      // Put the data first
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      // Now get it to retrieve some attributes
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      String lastMod = get.getResponseHeader("Last-Modified").getValue();

      // Attempt to delete using the If-Modified-Since header with the lastMod we got back from the get
      DeleteMethod delete = new DeleteMethod(fullPathKey);
      delete.setRequestHeader("If-Modified-Since", lastMod);
      assertEquals(HttpServletResponse.SC_NOT_MODIFIED, call(delete).getStatusCode());

      // Try to delete again, but with an older last modification date
      DeleteMethod deleteAgain = new DeleteMethod(fullPathKey);
      String dateMinus = addDay(lastMod, -1);
      deleteAgain.setRequestHeader("If-Modified-Since", dateMinus);
      assertEquals(HttpServletResponse.SC_OK, call(deleteAgain).getStatusCode());
   }

   public void testDeleteDataWithIfUnmodifiedSince(Method m) throws Exception {
      // Put the data first
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      // Now get it to retrieve some attributes
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      String lastMod = get.getResponseHeader("Last-Modified").getValue();

      // Attempt to delete using the If-Unmodified-Since header with a date earlier than the one we got back from the GET
      DeleteMethod delete = new DeleteMethod(fullPathKey);
      String dateMinus = addDay(lastMod, -1);
      delete.setRequestHeader("If-Unmodified-Since", dateMinus);
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(delete).getStatusCode());

      // Try to delete again, but with an older last modification date
      DeleteMethod deleteAgain = new DeleteMethod(fullPathKey);
      deleteAgain.setRequestHeader("If-Unmodified-Since", lastMod);
      assertEquals(HttpServletResponse.SC_OK, call(deleteAgain).getStatusCode());
   }

   public void testDeleteCachePreconditionUnimplemented(Method m) throws Exception {
      testDeletePreconditionalUnimplemented(fullPath);
   }

   private void testDeletePreconditionalUnimplemented(String fullPathKey) throws Exception {
      testDeletePreconditionalUnimplemented(fullPathKey, "If-Match");
      testDeletePreconditionalUnimplemented(fullPathKey, "If-None-Match");
      testDeletePreconditionalUnimplemented(fullPathKey, "If-Modified-Since");
      testDeletePreconditionalUnimplemented(fullPathKey, "If-Unmodified-Since");
   }

   private void testDeletePreconditionalUnimplemented(
         String fullPathKey, String preconditionalHeaderName) throws Exception {
      DeleteMethod delete = new DeleteMethod(fullPathKey);
      delete.setRequestHeader(preconditionalHeaderName, "*");
      call(delete);

      assertNotImplemented(delete);
   }

   private void assertNotImplemented(HttpMethod method) throws IOException {
      assertEquals(method.getStatusCode(), 501);
      assertEquals(method.getStatusText(), "Not Implemented");
      assert (method.getResponseBodyAsString().toLowerCase().contains("precondition"));
   }

   public void testRemoveEntry(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PostMethod put = new PostMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      assertEquals(HttpServletResponse.SC_OK, call(new HeadMethod(fullPathKey)).getStatusCode());

      call(new DeleteMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_NOT_FOUND, call(new HeadMethod(fullPathKey)).getStatusCode());
   }

   public void testWipeCacheBucket(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PostMethod put = new PostMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      PostMethod put_ = new PostMethod(fullPathKey + "2");
      put_.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put_);

      assertEquals(HttpServletResponse.SC_OK, call(new HeadMethod(fullPathKey)).getStatusCode());
      call(new DeleteMethod(fullPath));
      assertEquals(HttpServletResponse.SC_NOT_FOUND, call(new HeadMethod(fullPathKey)).getStatusCode());
   }

   public void testAsyncAddRemove(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PostMethod put = new PostMethod(fullPathKey);
      put.setRequestHeader("performAsync", "true");
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);

      Thread.sleep(50);
      assertEquals(HttpServletResponse.SC_OK, call(new HeadMethod(fullPathKey)).getStatusCode());

      DeleteMethod del = new DeleteMethod(fullPathKey);
      del.setRequestHeader("performAsync", "true");
      call(del);
      Thread.sleep(50);
      assertEquals(HttpServletResponse.SC_NOT_FOUND, call(new HeadMethod(fullPathKey)).getStatusCode());
   }

   public void testShouldCopeWithSerializable(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      call(new GetMethod(fullPathKey));

      MySer obj = new MySer();
      obj.name = "mic";
      getCacheManager("single").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME).put(m.getName(), obj);
      getCacheManager("single").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME).put(m.getName() + "2", "hola");
      getCacheManager("single").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME).put(m.getName() + "3", new MyNonSer());

      //check we can get it back as an object...
      GetMethod get = new GetMethod(fullPathKey);
      get.setRequestHeader("Accept", "application/x-java-serialized-object");
      call(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      ObjectInputStream in = new ObjectInputStream(get.getResponseBodyAsStream());
      MySer res = (MySer) in.readObject();
      assertNotNull(res);
      assertEquals("mic", res.name);
      assertEquals("application/x-java-serialized-object", get.getResponseHeader("Content-Type").getValue());

      HttpMethodBase getStr = call(new GetMethod(fullPathKey + "2"));
      assertEquals("hola", getStr.getResponseBodyAsString());
      assertEquals("text/plain", getStr.getResponseHeader("Content-Type").getValue());

      //now check we can get it back as JSON if we want...
      get.setRequestHeader("Accept", "application/json");
      call(get);
      assertEquals("{\"name\":\"mic\"}", get.getResponseBodyAsString());
      assertEquals("application/json", get.getResponseHeader("Content-Type").getValue());

      //and why not XML
      get.setRequestHeader("Accept", "application/xml");
      call(get);
      assertEquals("application/xml", get.getResponseHeader("Content-Type").getValue());
      assertTrue(get.getResponseBodyAsString().indexOf("<org.infinispan.rest.MySer>") > -1);

      //now check we can get it back as JSON if we want...
      GetMethod get3 = new GetMethod(fullPathKey + "3");
      get3.setRequestHeader("Accept", "application/json");
      call(get3);
      assertEquals("{\"name\":\"mic\"}", get3.getResponseBodyAsString());
      assertEquals("application/json", get3.getResponseHeader("Content-Type").getValue());
   }

   public void testInsertSerializableObjects(Method m) throws Exception {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      new ObjectOutputStream(bout).writeObject(new MySer());
      put(fullPathKey(m), bout.toByteArray(), "application/x-java-serialized-object");
      getCacheManager("single").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME)
            .get(m.getName());
   }

   public void testNonexistentCache(Method m) throws Exception {
      String fullPathKey = HOST + "/rest/nonexistent/" + m.getName();
      GetMethod get = new GetMethod(fullPathKey);
      call(get);
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode());

      HttpMethodBase head = call(new HeadMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_NOT_FOUND, head.getStatusCode());

      PostMethod put = new PostMethod(fullPathKey);
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"));
      call(put);
      assertEquals(HttpServletResponse.SC_NOT_FOUND, put.getStatusCode());
   }

   public void testByteArrayAsSerializedObjects(Method m) throws Exception {
      sendByteArrayAs(m, "application/x-java-serialized-object");
   }

   public void testByteArrayAsOctecStreamObjects(Method m) throws Exception {
      sendByteArrayAs(m, "application/octet-stream");
   }

   private void sendByteArrayAs(Method m, String contentType) throws Exception {
      byte[] serializedOnClient = new byte[]{0x65, 0x66, 0x67};
      put(fullPathKey(m), serializedOnClient, contentType);
      BufferedInputStream dataRead = new BufferedInputStream(
            get(m, Optional.empty(), Optional.of(contentType)).getResponseBodyAsStream());
      byte[] bytesRead = new byte[3];
      dataRead.read(bytesRead);
      assertEquals(serializedOnClient, bytesRead);
   }

   public void testIfUnmodifiedSince(Method m) throws Exception {
      put(m);
      HttpMethodBase result = get(m);
      String dateLast = result.getResponseHeader("Last-Modified").getValue();
      String dateMinus = addDay(dateLast, -1);
      String datePlus = addDay(dateLast, 1);
      assertNotNull(get(m, Optional.of(dateLast)).getResponseBodyAsString());
      assertNotNull(get(m, Optional.of(datePlus)).getResponseBodyAsString());
      result = get(m, Optional.of(dateMinus), Optional.empty(), HttpServletResponse.SC_PRECONDITION_FAILED);
   }

   public void testETagChanges(Method m) throws Exception {
      put(m, "data1");
      String eTagFirst = get(m).getResponseHeader("ETag").getValue();
      // second get should get the same ETag
      assertEquals(eTagFirst, get(m).getResponseHeader("ETag").getValue());
      // do second PUT
      put(m, "data2");
      // get ETag again
      String eTagSecond = get(m).getResponseHeader("ETag").getValue();
      assertFalse("etag1 %s; etag2 %s; equals? %b".format(
            eTagFirst, eTagSecond, eTagFirst.equals(eTagSecond)),
            eTagFirst.equals(eTagSecond));
   }

   public void testConcurrentETagChanges(Method m) throws Exception {
      CountDownLatch v2PutLatch = new CountDownLatch(1);
      CountDownLatch v3PutLatch = new CountDownLatch(1);
      CountDownLatch v2FinishLatch = new CountDownLatch(1);

      EmbeddedCacheManager cacheManager = createCacheManager();
      ControlledCacheManager mockCacheManager = new ControlledCacheManager(cacheManager, v2PutLatch, v3PutLatch, v2FinishLatch);
      EmbeddedRestServer server = RestTestingUtil.startRestServer(mockCacheManager);
      put(fullPathWithPort(m, server.getPort()), "data1", "application/text");
      try {
         Future replaceFuture = fork(() -> {
            // Put again, with a different client (separate thread);
            HttpClient newClient = new HttpClient();
            PutMethod put = new PutMethod(fullPathWithPort(m, server.getPort()));
            put.setRequestHeader("Content-Type", "application/text");
            put.setRequestEntity(new StringRequestEntity("data2", null, null));
            newClient.executeMethod(put);
            assertEquals(HttpServletResponse.SC_OK, put.getStatusCode());

            // 5. v2 applied, let v3 finish
            v2FinishLatch.countDown();
            return null;
         });
         // 1. Wait for v3 to be allowed
         boolean cont = v3PutLatch.await(10, TimeUnit.SECONDS);
         assertTrue("Timed out waiting for concurrent put", cont);
         // Ready to do concurrent put which should not be allowed
         PutMethod put = new PutMethod(fullPathWithPort(m, server.getPort()));
         put.setRequestHeader("Content-Type", "application/text");
         put.setRequestEntity(new StringRequestEntity("data3", null, null));
         call(put);
         assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED,
               put.getStatusCode());

         // Wait for replace to happen
         replaceFuture.get(5, TimeUnit.SECONDS);
         // Final data should be v2
         assertEquals("data2", get(m).getResponseBodyAsString());
      } finally {
         server.stop();
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testSerializedStringGetBytes(Method m) throws Exception {
      byte[] data = ("v-" + m.getName()).getBytes("UTF-8");

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream oo = new ObjectOutputStream(bout);
      oo.writeObject(data);
      oo.flush();

      byte[] bytes = bout.toByteArray();
      put(fullPathKey(m), bytes, "application/x-java-serialized-object");

      byte[] bytesRead = get(m, Optional.empty(), Optional.of("application/x-java-serialized-object")).getResponseBody();
      assertTrue(Arrays.equals(bytes, bytesRead));

      ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytesRead));
      byte[] dataBack = (byte[]) oin.readObject();
      assertTrue(Arrays.equals(data, dataBack));
   }

   public void testDefaultConfiguredExpiryValues(Method m) throws Exception {
      String cacheName = "evictExpiryCache";
      String fullPathKey = String.format("%s/rest/%s/%s", HOST, cacheName, m.getName());
      PostMethod post = new PostMethod(fullPathKey);
      // Live forever...
      post.setRequestHeader("timeToLiveSeconds", "-1");
      post.setRequestEntity(new StringRequestEntity("data", "text/plain", "UTF-8"));
      call(post);
      // Sleep way beyond the default in the config
      Thread.sleep(5000);
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals("data", get.getResponseBodyAsString());
      assertNull(get.getResponseHeader("Expires"));

      long startTime = System.currentTimeMillis();
      int lifespan = 3000;
      fullPathKey = fullPathKey + "-2";
      post = new PostMethod(fullPathKey);
      post.setRequestHeader("Content-Type", "application/text");
      // It'll fallback on configured lifespan/maxIdle values.
      post.setRequestHeader("timeToLiveSeconds", "0");
      post.setRequestHeader("maxIdleTimeSeconds", "0");
      post.setRequestEntity(new StringRequestEntity("data2", "text/plain", "UTF-8"));
      call(post);
      while (System.currentTimeMillis() < startTime + lifespan) {
         get = call(new GetMethod(fullPathKey));
         String response = get.getResponseBodyAsString();
         // The entry could have expired before our request got to the server
         // Scala doesn't support break, so we need to test the current time twice
         if (System.currentTimeMillis() < startTime + lifespan) {
            assertEquals("data2", response);
            assertNotNull(get.getResponseHeader("Expires"));
            Thread.sleep(100);
         }
      }

      // Make sure that in the next 20 secs data is removed
      waitNotFound(startTime, lifespan, fullPathKey);
      assertEquals(SC_NOT_FOUND, call(new GetMethod(fullPathKey)).getStatusCode());

      startTime = System.currentTimeMillis();
      lifespan = 0;
      fullPathKey = "%s-3".format(fullPathKey);
      post = new PostMethod(fullPathKey);
      // Will use the configured lifespan
      post.setRequestHeader("timeToLiveSeconds", "0");
      post.setRequestEntity(new StringRequestEntity("data3", "text/plain", "UTF-8"));
      call(post);
      Thread.sleep(1000);
      get = call(new GetMethod(fullPathKey));
      String response = get.getResponseBodyAsString();
      assertEquals("data3", response);
      Thread.sleep(2000);
      assertEquals(HttpServletResponse.SC_NOT_FOUND, call(new GetMethod(fullPathKey)).getStatusCode());

      fullPathKey = "%s-4".format(fullPathKey);
      post = new PostMethod(fullPathKey);
      // It will use configured maxIdle
      post.setRequestHeader("maxIdleTimeSeconds", "0");
      post.setRequestEntity(new StringRequestEntity("data4", "text/plain", "UTF-8"));
      call(post);
      // Sleep way beyond the default in the config
      Thread.sleep(2500);
      assertEquals(HttpServletResponse.SC_NOT_FOUND, call(new GetMethod(fullPathKey)).getStatusCode());
   }

   public void testCacheControlResponseHeader(Method m) throws Exception {
      String cacheName = "evictExpiryCache";
      String fullPathKey = String.format("%s/rest/%s/%s", HOST, cacheName, m.getName());
      PostMethod post = new PostMethod(fullPathKey);

      post.setRequestHeader("timeToLiveSeconds", "10");
      post.setRequestEntity(new StringRequestEntity("data", "text/plain", "UTF-8"));
      call(post);
      Thread.sleep(2000);
      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals("data", get.getResponseBodyAsString());
      assertNotNull(get.getResponseHeader("Cache-Control"));
      int retrievedMaxAge = CacheControl.valueOf(get.getResponseHeader("Cache-Control").getValue()).getMaxAge();
      assertTrue(retrievedMaxAge > 0);
   }

   public void testGetCacheControlMinFreshRequestHeader(Method m) throws Exception {
      String cacheName = "evictExpiryCache";
      String fullPathKey = String.format("%s/rest/%s/%s", HOST, cacheName, m.getName());
      PostMethod post = new PostMethod(fullPathKey);

      post.setRequestHeader("timeToLiveSeconds", "10");
      post.setRequestEntity(new StringRequestEntity("data", "text/plain", "UTF-8"));
      call(post);
      Thread.sleep(2000);

      GetMethod getLongMinFresh = new GetMethod(fullPathKey);
      getLongMinFresh.addRequestHeader("Cache-Control", "no-transform, min-fresh=20");
      HttpMethodBase getResp = call(getLongMinFresh);
      assertEquals(HttpServletResponse.SC_NOT_FOUND, getResp.getStatusCode());

      GetMethod getShortMinFresh = new GetMethod(fullPathKey);
      getShortMinFresh.addRequestHeader("Cache-Control", "no-transform, min-fresh=2");
      getResp = call(getShortMinFresh);
      assertNotNull(getResp.getResponseHeader("Cache-Control"));
      assertEquals("data", getResp.getResponseBodyAsString());

      GetMethod getNoMinFresh = new GetMethod(fullPathKey);
      getResp = call(getNoMinFresh);
      assertNotNull(getResp.getResponseHeader("Cache-Control"));
      assertEquals("data", getResp.getResponseBodyAsString());
   }

   public void testHeadCacheControlMinFreshRequestHeader(Method m) throws Exception {
      String cacheName = "evictExpiryCache";
      String fullPathKey = String.format("%s/rest/%s/%s", HOST, cacheName, m.getName());
      PostMethod post = new PostMethod(fullPathKey);

      post.setRequestHeader("timeToLiveSeconds", "10");
      post.setRequestEntity(new StringRequestEntity("data", "text/plain", "UTF-8"));
      call(post);
      Thread.sleep(2000);

      HeadMethod headLongMinFresh = new HeadMethod(fullPathKey);
      headLongMinFresh.addRequestHeader("Cache-Control", "no-transform, min-fresh=20");
      HttpMethodBase headResp = call(headLongMinFresh);
      assertEquals(HttpServletResponse.SC_NOT_FOUND, headResp.getStatusCode());

      HeadMethod headShortMinFresh = new HeadMethod(fullPathKey);
      headShortMinFresh.addRequestHeader("Cache-Control", "no-transform, min-fresh=2");
      headResp = call(headShortMinFresh);
      int retrievedMaxAge = CacheControl.valueOf(headResp.getResponseHeader("Cache-Control").getValue()).getMaxAge();
      assertTrue(retrievedMaxAge > 0);

      HeadMethod headNoMinFresh = new HeadMethod(fullPathKey);
      headResp = call(headNoMinFresh);
      assertNotNull(headResp.getResponseHeader("Cache-Control"));
   }

   public void testPutByteArrayTwice(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      byte[] data = new byte[]{42, 42, 42};

      put.setRequestEntity(new ByteArrayRequestEntity(data, "application/x-java-serialized-object"));
      assertEquals(HttpServletResponse.SC_OK, call(put).getStatusCode());

      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());

      PutMethod reput = new PutMethod(fullPathKey);
      reput.setRequestEntity(new ByteArrayRequestEntity(data, "application/x-java-serialized-object"));
      assertEquals(HttpServletResponse.SC_OK, call(reput).getStatusCode());
   }

   public void testDeleteSerializedObject(Method m) throws Exception {
      String fullPathKey = fullPath + "/" + m.getName();
      PutMethod put = new PutMethod(fullPathKey);
      byte[] data = new byte[]{42, 42, 42};

      put.setRequestEntity(new ByteArrayRequestEntity(data, "application/x-java-serialized-object"));
      assertEquals(HttpServletResponse.SC_OK, call(put).getStatusCode());

      HttpMethodBase get = call(new GetMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());

      HttpMethodBase delete = call(new DeleteMethod(fullPathKey));
      assertEquals(HttpServletResponse.SC_OK, delete.getStatusCode());
   }

   public void testDisableCache(Method m) throws Exception {
      Callable<HttpMethodBase> doGet = () -> call(new GetMethod(fullPathKey(m)));

      put(m);
      assertEquals(SC_OK, doGet.call().getStatusCode());

      ignoreCache(cacheName);
      assertEquals(SC_INTERNAL_SERVER_ERROR, doGet.call().getStatusCode());

      enableCache(cacheName);
      assertEquals(SC_OK, doGet.call().getStatusCode());
   }

   private void waitNotFound(Long startTime, int lifespan, String fullPathKey) throws Exception {
      if (System.currentTimeMillis() < startTime + lifespan + 20000) {
         if (SC_NOT_FOUND != (call(new GetMethod(fullPathKey)).getStatusCode())) {
            Thread.sleep(100);
            waitNotFound(startTime, lifespan, fullPathKey); // Good ol' tail recursion :);
         }
      }
   }

   private HttpMethodBase put(Method m) throws Exception {
      return put(fullPathKey(m), "data", "application/text");
   }

   private HttpMethodBase put(Method m, Object data) throws Exception {
      return put(fullPathKey(m), data, "application/text");
   }

   private HttpMethodBase put(String path, Object data, String contentType) throws Exception {
      PutMethod put = new PutMethod(path);
      put.setRequestHeader("Content-Type", contentType);
      RequestEntity reqEntity;
      if (data instanceof String) {
         reqEntity = new StringRequestEntity((String) data, null, null);
      } else if (data instanceof byte[]) {
         reqEntity = new InputStreamRequestEntity(new ByteArrayInputStream((byte[]) data));
      } else {
         throw new IllegalArgumentException("Only String or byte[] allowed, received: " + data.getClass());
      }
      put.setRequestEntity(reqEntity);
      call(put);
      assertEquals(HttpServletResponse.SC_OK, put.getStatusCode());
      return put;
   }

   private HttpMethodBase get(Method m) throws Exception {
      return get(m, Optional.empty());
   }

   private HttpMethodBase get(Method m, Optional<String> unmodSince) throws Exception {
      return get(m, unmodSince, Optional.empty(), HttpServletResponse.SC_OK);
   }

   private HttpMethodBase get(Method m, Optional<String> unmodSince, Optional<String> acceptType) throws Exception {
      return get(m, unmodSince, acceptType, HttpServletResponse.SC_OK);
   }

   private HttpMethodBase get(Method m, Optional<String> unmodSince, Optional<String> acceptType, int expCode) throws Exception {
      GetMethod get = new GetMethod(fullPathKey(m));
      if (unmodSince.isPresent())
         get.setRequestHeader("If-Unmodified-Since", unmodSince.get());
      if (acceptType.isPresent())
         get.setRequestHeader("Accept", acceptType.get());
      call(get);
      assertEquals(expCode, get.getStatusCode());
      return get;
   }

   private String fullPathKey(Method m) {
      return fullPath + "/" + m.getName();
   }

   private String fullPathKey(Method m, int port) {
      return fullPath + "/" + m.getName();
   }

   String addDay(String aDate, int days) throws ParseException {
      DateFormat format = new SimpleDateFormat(DATE_PATTERN_RFC1123, Locale.US);
      Date date = format.parse(aDate);
      Calendar cal = Calendar.getInstance();
      cal.setTime(date);
      cal.add(Calendar.DATE, days);
      return format.format(cal.getTime());
   }

   class ControlledCacheManager extends AbstractDelegatingEmbeddedCacheManager {
      private final CountDownLatch v2PutLatch;
      private final CountDownLatch v3PutLatch;
      private final CountDownLatch v2FinishLatch;

      public ControlledCacheManager(EmbeddedCacheManager cm, CountDownLatch v2PutLatch, CountDownLatch v3PutLatch,
                                    CountDownLatch v2FinishLatch) {
         super(cm);
         this.v2PutLatch = v2PutLatch;
         this.v3PutLatch = v3PutLatch;
         this.v2FinishLatch = v2FinishLatch;
      }

      @Override
      public <K, V> Cache<K, V> getCache() {
         return (Cache<K, V>) new ControlledCache(super.getCache(),
               v2PutLatch, v3PutLatch, v2FinishLatch);
      }
   }

   class ControlledCache extends AbstractDelegatingAdvancedCache<String, Object> {
      private final CountDownLatch v2PutLatch;
      private final CountDownLatch v3PutLatch;
      private final CountDownLatch v2FinishLatch;

      public ControlledCache(Cache<String, Object> cache, CountDownLatch v2PutLatch, CountDownLatch v3PutLatch,
                             CountDownLatch v2FinishLatch) {
         super(cache.getAdvancedCache());
         this.v2PutLatch = v2PutLatch;
         this.v3PutLatch = v3PutLatch;
         this.v2FinishLatch = v2FinishLatch;
      }

      @Override
      public boolean replace(String key, Object oldValue, Object value, Metadata metadata) {
         byte[] newByteArray = (byte[]) value;
         byte[] oldByteArray = (byte[]) oldValue;
         String oldAsString = new String(oldByteArray);
         String newAsString = new String(newByteArray);
         try {
            if (Arrays.equals(newByteArray, "data2".getBytes())) {
               log.debug("Let v3 apply...");
               v3PutLatch.countDown(); // 2. Let the v3 put come in
               log.debug("Wait until v2 can be stored");
               // 3. Wait until v2 can apply
               boolean cont = v2PutLatch.await(10, TimeUnit.SECONDS);
               if (!cont)
                  fail("Timed out waiting for v2 to be allowed");
            } else if (Arrays.equals(newByteArray, "data3".getBytes())) {
               log.debugf("About to store v3, let v2 apply, oldValue(for v3)=%s",
                     oldAsString);
               // 4. Let data2 apply
               v2PutLatch.countDown();
               v2FinishLatch.await(10, TimeUnit.SECONDS); // Wait for data2 apply
            }
         } catch (InterruptedException e) {
            throw new CacheException(e);
         }
         log.debugf("Replace key=%s, oldValue=%s, value=%s",
               key, oldAsString, newAsString);

         return super.replace(key, oldValue, value, metadata);
      }
   }

}

class MyNonSer {
   String name = "mic";

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }
}

class MySer extends MyNonSer implements Serializable {
}
