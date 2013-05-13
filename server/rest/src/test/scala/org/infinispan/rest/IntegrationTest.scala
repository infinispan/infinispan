/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.rest

import org.apache.commons.httpclient.methods._
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponse._
import org.infinispan.remoting.MIMECacheEntry
import java.io._
import org.testng.annotations.{Test, BeforeClass, AfterClass}
import java.lang.reflect.Method
import org.infinispan.api.BasicCacheContainer
import scala.math._
import org.infinispan.test.TestingUtil
import java.text.SimpleDateFormat
import org.apache.commons.httpclient.{HttpMethodBase, Header, HttpClient}
import org.apache.commons.httpclient.HttpMethod
import java.util.{Calendar, Locale}
import org.testng.AssertJUnit._
import org.infinispan.manager.{EmbeddedCacheManager, AbstractDelegatingEmbeddedCacheManager}
import org.infinispan.{Metadata, Cache, AdvancedCache, AbstractDelegatingAdvancedCache}
import java.util.concurrent.{TimeUnit, CountDownLatch}
import scala.concurrent.ops._
import org.infinispan.server.core.logging.JavaLog
import org.infinispan.util.logging.LogFactory
import org.infinispan.test.fwk.TestCacheManagerFactory
import java.util

/**
 * This tests using the Apache HTTP commons client library - but you could use anything
 * Decided to do this instead of testing the Server implementation itself, as testing the impl directly was kind of too easy.
 * (Given that RESTEasy does most of the heavy lifting !).
 *
 * @author Michael Neale
 * @author Galder Zamarreño
 * @author Michal Linhard
 * @since 4.0
 */
@Test(groups = Array("functional"), testName = "rest.IntegrationTest")
class IntegrationTest extends RESTServerTestBase {

   private lazy val log: JavaLog = LogFactory.getLog(getClass, classOf[JavaLog])

   val HOST = "http://localhost:8888"
   val cacheName = BasicCacheContainer.DEFAULT_CACHE_NAME
   val fullPath = HOST + "/rest/" + cacheName
   val DATE_PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz"

   //val HOST = "http://localhost:8080/infinispan/"

   @BeforeClass
   def setUp() {
      addServer("single", 8888, TestCacheManagerFactory.fromXml("test-config.xml"))
      startServers()
      createClient()
   }

   @AfterClass(alwaysRun = true)
   def tearDown() {
      stopServers()
      destroyClient()
   }

   def testBasicOperation(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName

      val insert = new PutMethod(fullPathKey)
      val initialXML = <hey>ho</hey>

      insert.setRequestEntity(new ByteArrayRequestEntity(initialXML
              .toString().getBytes, "application/octet-stream"))

      call(insert)

      assertEquals("", insert.getResponseBodyAsString.trim)
      assertEquals(HttpServletResponse.SC_OK, insert.getStatusCode)

      val get = new GetMethod(fullPathKey)
      call(get)
      val bytes = get.getResponseBody
      assertEquals(bytes.size, initialXML.toString().getBytes.size)
      assertEquals(<hey>ho</hey>.toString(), get.getResponseBodyAsString)
      val hdr: Header = get.getResponseHeader("Content-Type")
      assertEquals("application/octet-stream", hdr.getValue)

      val remove = new DeleteMethod(fullPathKey)
      call(remove)
      call(get)

      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)

      call(insert)
      call(get)
      assertEquals(<hey>ho</hey>.toString(), get.getResponseBodyAsString)

      val removeAll = new DeleteMethod(fullPath)
      assertEquals(HttpServletResponse.SC_OK, call(removeAll).getStatusCode)

      call(get)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)

      val bout = new ByteArrayOutputStream
      val oo = new ObjectOutputStream(bout)
      oo.writeObject(new MIMECacheEntry("foo", "hey".getBytes))
      oo.flush()
      val byteData = bout.toByteArray

      val insertMore = new PutMethod(fullPathKey)

      insertMore.setRequestEntity(new ByteArrayRequestEntity(byteData,  "application/octet-stream"))

      call(insertMore)

      val getMore = new GetMethod(fullPathKey)
      call(getMore)

      val bytesBack = getMore.getResponseBody
      assertEquals(byteData.length, bytesBack.length)

      val oin = new ObjectInputStream(new ByteArrayInputStream(bytesBack))
      val ce = oin.readObject.asInstanceOf[MIMECacheEntry]
      assertEquals("foo", ce.contentType)
   }

   def testEmptyGet() {
      assertEquals(
         HttpServletResponse.SC_NOT_FOUND,
         call(new GetMethod(HOST + "/rest/" + cacheName + "/nodata")).getStatusCode
      )
   }

   def testGetCollection() {
      val post_a = new PostMethod(s"$fullPath/a")
      post_a.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      call(post_a)
      val post_b = new PostMethod(s"$fullPath/b")
      post_b.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      call(post_b)

      val html = getCollection("text/html")
      assertTrue(html.contains("<a href=\"a\">a</a>"))
      assertTrue(html.contains("<a href=\"b\">b</a>"))

      val xml = getCollection("application/xml")
      assertTrue(xml.contains("<key>a</key>"))
      assertTrue(xml.contains("<key>b</key>"))

      val plain = getCollection("text/plain")
      assertTrue(plain.contains("a\n"))
      assertTrue(plain.contains("b\n"))

      val json = getCollection("application/json")
      assertTrue(json.contains("\"a\""))
      assertTrue(json.contains("\"b\""))
   }

   def testGetCollectionEscape() {
      val post_a = new PostMethod(s"$fullPath/%22a%22")
      post_a.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      call(post_a)
      val post_b = new PostMethod(s"$fullPath/b%3E")
      post_b.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      call(post_b)

      val html = getCollection("text/html")
      assertTrue(html.contains("<a href=\"&quot;a&quot;\">&quot;a&quot;</a>"))
      assertTrue(html.contains("<a href=\"b&gt;\">b&gt;</a>"))

      val xml = getCollection("application/xml")
      assertTrue(xml.contains("<key>&quot;a&quot;</key>"))
      assertTrue(xml.contains("<key>b&gt;</key>"))

      val plain = getCollection("text/plain")
      assertTrue(plain.contains("\"a\"\n"))
      assertTrue(plain.contains("b>\n"))

      val json = getCollection("application/json")
      assertTrue(json.contains("\\\"a\\\""))
      assertTrue(json.contains("\"b>\""))
   }

   private def getCollection(variant: String): String = {
      val get = new GetMethod(fullPath)
      get.addRequestHeader("Accept", variant)
      val coll = call(get)
      assertEquals(HttpServletResponse.SC_OK, coll.getStatusCode)
      assertEquals(variant, coll.getResponseHeader("Content-Type").getValue)
      coll.getResponseBodyAsString
   }

   def testGet(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      call(post)

      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      assertNotNull(get.getResponseHeader("ETag").getValue)
      assertNotNull(get.getResponseHeader("Last-Modified").getValue)
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)
      assertEquals("data", get.getResponseBodyAsString)
   }

   def testHead(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      call(post)

      val get = call(new HeadMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      assertNotNull(get.getResponseHeader("ETag").getValue)
      assertNotNull(get.getResponseHeader("Last-Modified").getValue)
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)

      assertNull(get.getResponseBodyAsString)
   }

   def testGetIfUnmodified(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      call(post)

      var get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      assertNotNull(get.getResponseHeader("ETag").getValue)
      val lastMod = get.getResponseHeader("Last-Modified").getValue
      assertNotNull(lastMod)
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)
      assertEquals("data", get.getResponseBodyAsString)

      // now get again
      val getAgain = new GetMethod(fullPathKey)
      getAgain.addRequestHeader("If-Unmodified-Since", lastMod)
      get = call(getAgain)
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      assertNotNull(get.getResponseHeader("ETag").getValue)
      assertNotNull(get.getResponseHeader("Last-Modified").getValue)
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)
      assertEquals("data", get.getResponseBodyAsString)
   }

   def testPostDuplicate(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(post)

      //Should get a conflict as its a DUPE post
      assertEquals(HttpServletResponse.SC_CONFLICT, call(post).getStatusCode)

      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))

      //Should be all ok as its a put
      assertEquals(HttpServletResponse.SC_OK, call(put).getStatusCode)
   }

   def testPutDataWithTimeToLive(m: Method) {
      putAndAssertEphemeralData(m, "2", "3")
   }

   def testPutDataWithMaxIdleOnly(m: Method) {
      putAndAssertEphemeralData(m, "", "3")
   }

   def testPutDataWithTimeToLiveOnly(m: Method) {
      putAndAssertEphemeralData(m, "3", "")
   }

   private def putAndAssertEphemeralData(m: Method, timeToLiveSeconds: String, maxIdleTimeSeconds: String) {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      var maxWaitTime = 0
      if (!timeToLiveSeconds.isEmpty) {
         maxWaitTime = max(maxWaitTime, timeToLiveSeconds.toInt)
         post.setRequestHeader("timeToLiveSeconds", timeToLiveSeconds)
      }

      if (!maxIdleTimeSeconds.isEmpty) {
         maxWaitTime = max(maxWaitTime, maxIdleTimeSeconds.toInt)
         post.setRequestHeader("maxIdleTimeSeconds", maxIdleTimeSeconds)
      }

      post.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(post)

      val get = call(new GetMethod(fullPathKey))
      assertEquals("data", get.getResponseBodyAsString)

      TestingUtil.sleepThread((maxWaitTime + 1) * 1000)
      call(get)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)
   }

   def testPutDataWithIfMatch(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      // Now get it to retrieve some attributes
      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val etag = get.getResponseHeader("ETag").getValue

      // Put again using the If-Match with the ETag we got back from the get
      val reput = new PutMethod(fullPathKey)
      reput.setRequestHeader("If-Match", etag)
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_OK, call(reput).getStatusCode)

      // Try to put again, but with a different ETag
      val reputAgain = new PutMethod(fullPathKey)
      reputAgain.setRequestHeader("If-Match", "x")
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(reputAgain).getStatusCode)
   }

   def testPutDataWithIfNoneMatch(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      // Now get it to retrieve some attributes
      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val etag = get.getResponseHeader("ETag").getValue

      // Put again using the If-Match with the ETag we got back from the get
      val reput = new PutMethod(fullPathKey)
      reput.setRequestHeader("If-None-Match", "x")
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_OK, call(reput).getStatusCode)

      // Try to put again, but with a different ETag
      val reputAgain = new PutMethod(fullPathKey)
      reputAgain.setRequestHeader("If-None-Match", etag)
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(reputAgain).getStatusCode)
   }

   def testPutDataWithIfModifiedSince(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      // Now get it to retrieve some attributes
      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val lastMod = get.getResponseHeader("Last-Modified").getValue

      // Put again using the If-Modified-Since with the lastMod we got back from the get
      val reput = new PutMethod(fullPathKey)
      reput.setRequestHeader("If-Modified-Since", lastMod)
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_NOT_MODIFIED, call(reput).getStatusCode)

      // Try to put again, but with an older last modification date
      val reputAgain = new PutMethod(fullPathKey)
      val dateMinus = addDay(lastMod, -1)
      reputAgain.setRequestHeader("If-Modified-Since", dateMinus)
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_OK, call(reputAgain).getStatusCode)
   }

   def testPutDataWithIfUnModifiedSince(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      // Now get it to retrieve some attributes
      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val lastMod = get.getResponseHeader("Last-Modified").getValue

      // Put again using the If-Unmodified-Since with a date earlier than the one we got back from the GET
      val reput = new PutMethod(fullPathKey)
      val dateMinus = addDay(lastMod, -1)
      reput.setRequestHeader("If-Unmodified-Since", dateMinus)
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(reput).getStatusCode)

      // Try to put again, but using the date returned by the GET
      val reputAgain = new PutMethod(fullPathKey)
      reputAgain.setRequestHeader("If-Unmodified-Since", lastMod)
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_OK, call(reputAgain).getStatusCode)
   }

   def testDeleteDataWithIfMatch(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      // Now get it to retrieve some attributes
      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val etag = get.getResponseHeader("ETag").getValue

      // Attempt to delete with a wrong ETag
      val delete = new DeleteMethod(fullPathKey)
      delete.setRequestHeader("If-Match", "x")
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(delete).getStatusCode)

      // Try to delete again, but with the proper ETag
      val deleteAgain = new DeleteMethod(fullPathKey)
      deleteAgain.setRequestHeader("If-Match", etag)
      assertEquals(HttpServletResponse.SC_OK, call(deleteAgain).getStatusCode)
   }

   def testDeleteDataWithIfNoneMatch(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      // Now get it to retrieve some attributes
      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val etag = get.getResponseHeader("ETag").getValue

      // Attempt to delete with the ETag
      val delete = new DeleteMethod(fullPathKey)
      delete.setRequestHeader("If-None-Match", etag)
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(delete).getStatusCode)

      // Try to delete again, but with a non-matching ETag
      val deleteAgain = new DeleteMethod(fullPathKey)
      deleteAgain.setRequestHeader("If-None-Match", "x")
      assertEquals(HttpServletResponse.SC_OK, call(deleteAgain).getStatusCode)
   }

   def testDeleteDataWithIfModifiedSince(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      // Now get it to retrieve some attributes
      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val lastMod = get.getResponseHeader("Last-Modified").getValue

      // Attempt to delete using the If-Modified-Since header with the lastMod we got back from the get
      val delete = new DeleteMethod(fullPathKey)
      delete.setRequestHeader("If-Modified-Since", lastMod)
      assertEquals(HttpServletResponse.SC_NOT_MODIFIED, call(delete).getStatusCode)

      // Try to delete again, but with an older last modification date
      val deleteAgain = new DeleteMethod(fullPathKey)
      val dateMinus = addDay(lastMod, -1)
      deleteAgain.setRequestHeader("If-Modified-Since", dateMinus)
      assertEquals(HttpServletResponse.SC_OK, call(deleteAgain).getStatusCode)
   }

   def testDeleteDataWithIfUnmodifiedSince(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      // Now get it to retrieve some attributes
      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val lastMod = get.getResponseHeader("Last-Modified").getValue

      // Attempt to delete using the If-Unmodified-Since header with a date earlier than the one we got back from the GET
      val delete = new DeleteMethod(fullPathKey)
      val dateMinus = addDay(lastMod, -1)
      delete.setRequestHeader("If-Unmodified-Since", dateMinus)
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, call(delete).getStatusCode)

      // Try to delete again, but with an older last modification date
      val deleteAgain = new DeleteMethod(fullPathKey)
      deleteAgain.setRequestHeader("If-Unmodified-Since", lastMod)
      assertEquals(HttpServletResponse.SC_OK, call(deleteAgain).getStatusCode)
   }

   def testDeleteCachePreconditionUnimplemented(m: Method) {
      testDeletePreconditionalUnimplemented(fullPath)
   }

   private def testDeletePreconditionalUnimplemented(fullPathKey: String) {
     testDeletePreconditionalUnimplemented(fullPathKey, "If-Match")
     testDeletePreconditionalUnimplemented(fullPathKey, "If-None-Match")
     testDeletePreconditionalUnimplemented(fullPathKey, "If-Modified-Since")
     testDeletePreconditionalUnimplemented(fullPathKey, "If-Unmodified-Since")
   }

   private def testDeletePreconditionalUnimplemented(
         fullPathKey: String, preconditionalHeaderName: String) {
      val delete = new DeleteMethod(fullPathKey)
      delete.setRequestHeader(preconditionalHeaderName, "*")
      call(delete)

      assertNotImplemented(delete)
   }

   private def assertNotImplemented(method: HttpMethod) {
      assertEquals(method.getStatusCode, 501)
      assertEquals(method.getStatusText, "Not Implemented")
      assert(method.getResponseBodyAsString.toLowerCase.contains("precondition"))
   }

   def testRemoveEntry(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PostMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      assertEquals(HttpServletResponse.SC_OK, call(new HeadMethod(fullPathKey)).getStatusCode)

      call(new DeleteMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_NOT_FOUND, call(new HeadMethod(fullPathKey)).getStatusCode)
   }

   def testWipeCacheBucket(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PostMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      val put_ = new PostMethod(fullPathKey + "2")
      put_.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put_)

      assertEquals(HttpServletResponse.SC_OK, call(new HeadMethod(fullPathKey)).getStatusCode)
      call(new DeleteMethod(fullPath))
      assertEquals(HttpServletResponse.SC_NOT_FOUND, call(new HeadMethod(fullPathKey)).getStatusCode)
   }

   def testAsyncAddRemove(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PostMethod(fullPathKey)
      put.setRequestHeader("performAsync", "true")
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)

      Thread.sleep(50)
      assertEquals(HttpServletResponse.SC_OK, call(new HeadMethod(fullPathKey)).getStatusCode)

      val del = new DeleteMethod(fullPathKey)
      del.setRequestHeader("performAsync", "true")
      call(del)
      Thread.sleep(50)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, call(new HeadMethod(fullPathKey)).getStatusCode)
   }

   def testShouldCopeWithSerializable(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      call(new GetMethod(fullPathKey))

      val obj = new MySer
      obj.name = "mic"
      getCacheManager("single").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME).put(m.getName, obj)
      getCacheManager("single").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME).put(m.getName + "2", "hola")
      getCacheManager("single").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME).put(m.getName + "3", new MyNonSer)

      //check we can get it back as an object...
      val get = new GetMethod(fullPathKey)
      get.setRequestHeader("Accept", "application/x-java-serialized-object")
      call(get)
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val in = new ObjectInputStream(get.getResponseBodyAsStream)
      val res = in.readObject.asInstanceOf[MySer]
      assertNotNull(res)
      assertEquals("mic", res.name)
      assertEquals("application/x-java-serialized-object", get.getResponseHeader("Content-Type").getValue)

      val getStr = call(new GetMethod(fullPathKey + "2"))
      assertEquals("hola", getStr.getResponseBodyAsString)
      assertEquals("text/plain", getStr.getResponseHeader("Content-Type").getValue)

      //now check we can get it back as JSON if we want...
      get.setRequestHeader("Accept", "application/json")
      call(get)
      assertEquals("""{"name":"mic"}""", get.getResponseBodyAsString)
      assertEquals("application/json", get.getResponseHeader("Content-Type").getValue)

      //and why not XML
      get.setRequestHeader("Accept", "application/xml")
      call(get)
      assertEquals("application/xml", get.getResponseHeader("Content-Type").getValue)
      assertTrue(get.getResponseBodyAsString.indexOf("<org.infinispan.rest.MySer>") > -1)

      //now check we can get it back as JSON if we want...
      val get3 = new GetMethod(fullPathKey + "3")
      get3.setRequestHeader("Accept", "application/json")
      call(get3)
      assertEquals("""{"name":"mic"}""", get3.getResponseBodyAsString)
      assertEquals("application/json", get3.getResponseHeader("Content-Type").getValue)
   }

   def testInsertSerializableObjects(m: Method) {
      val bout = new ByteArrayOutputStream
      new ObjectOutputStream(bout).writeObject(new MySer)
      put(m, bout.toByteArray, "application/x-java-serialized-object")
      getCacheManager("single").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME)
              .get(m.getName).asInstanceOf[Array[Byte]]
   }

   def testNonexistentCache(m: Method) {
      val fullPathKey = HOST + "/rest/nonexistent/" + m.getName
      val get = new GetMethod(fullPathKey)
      call(get)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)

      val head = call(new HeadMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_NOT_FOUND, head.getStatusCode)

      val put = new PostMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      call(put)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, put.getStatusCode)
   }

   def testByteArrayAsSerializedObjects(m: Method) {
      sendByteArrayAs(m, "application/x-java-serialized-object")
   }

   def testByteArrayAsOctecStreamObjects(m: Method) {
      sendByteArrayAs(m, "application/octet-stream")
   }

   private def sendByteArrayAs(m: Method, contentType: String) {
      val serializedOnClient = Array[Byte](0x65, 0x66, 0x67)
      put(m, serializedOnClient, contentType)
      val dataRead = new BufferedInputStream(
         get(m, None, Some(contentType)).getResponseBodyAsStream)
      val bytesRead = new Array[Byte](3)
      dataRead.read(bytesRead)
      assertEquals(serializedOnClient, bytesRead)
   }

   def testIfUnmodifiedSince(m: Method) {
      put(m)
      var result = get(m)
      val dateLast = result.getResponseHeader("Last-Modified").getValue
      val dateMinus = addDay(dateLast, -1)
      val datePlus = addDay(dateLast, 1)
      assertNotNull(get(m, Some(dateLast)).getResponseBodyAsString)
      assertNotNull(get(m, Some(datePlus)).getResponseBodyAsString)
      result = get(m, Some(dateMinus), None, HttpServletResponse.SC_PRECONDITION_FAILED)
   }

   def testETagChanges(m: Method) {
      put(m, "data1")
      val eTagFirst = get(m).getResponseHeader("ETag").getValue
      // second get should get the same ETag
      assertEquals(eTagFirst, get(m).getResponseHeader("ETag").getValue)
      // do second PUT
      put(m, "data2")
      // get ETag again
      val eTagSecond = get(m).getResponseHeader("ETag").getValue
      assertFalse("etag1 %s; etag2 %s; equals? %b".format(
         eTagFirst, eTagSecond, eTagFirst.equals(eTagSecond)),
         eTagFirst.equals(eTagSecond))
   }

   def testConcurrentETagChanges(m: Method) {
      put(m, "data1")

      val v2PutLatch = new CountDownLatch(1)
      val v3PutLatch = new CountDownLatch(1)
      val v2FinishLatch = new CountDownLatch(1)
      val origCacheManager = getCacheManager("single")
      val mockCacheManager = new ControlledCacheManager(origCacheManager, v2PutLatch, v3PutLatch, v2FinishLatch)
      val knownCaches = getManagerInstance("single").knownCaches
      try {
         knownCaches.put(
            BasicCacheContainer.DEFAULT_CACHE_NAME,
            mockCacheManager.getCache[String, Array[Byte]]())

         val replaceFuture = future {
            // Put again, with a different client (separate thread)
            val newClient = new HttpClient
            val put = new PutMethod(fullPathKey(m))
            put.setRequestHeader("Content-Type", "application/text")
            put.setRequestEntity(new StringRequestEntity("data2", null, null))
            newClient.executeMethod(put)
            assertEquals(HttpServletResponse.SC_OK, put.getStatusCode)

            // 5. v2 applied, let v3 finish
            v2FinishLatch.countDown()
         }
         // 1. Wait for v3 to be allowed
         val continue = v3PutLatch.await(10, TimeUnit.SECONDS)
         assertTrue("Timed out waiting for concurrent put", continue)
         // Ready to do concurrent put which should not be allowed
         val put = new PutMethod(fullPathKey(m))
         put.setRequestHeader("Content-Type", "application/text")
         put.setRequestEntity(new StringRequestEntity("data3", null, null))
         call(put)
         assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED,
            put.getStatusCode)

         // Wait for replace to happen
         replaceFuture.apply()
         // Final data should be v2
         assertEquals("data2", get(m).getResponseBodyAsString)
      } finally {
         knownCaches.put(
            BasicCacheContainer.DEFAULT_CACHE_NAME,
            origCacheManager.getCache[String, Array[Byte]]().getAdvancedCache)
      }

   }

   def testSerializedStringGetBytes(m: Method) {
      val data = ("v-" + m.getName).getBytes("UTF-8")

      val bout = new ByteArrayOutputStream()
      val oo = new ObjectOutputStream(bout)
      oo.writeObject(data)
      oo.flush()

      val bytes = bout.toByteArray
      put(m, bytes, "application/x-java-serialized-object")

      val bytesRead = get(m, None, Some("application/x-java-serialized-object")).getResponseBody
      assertTrue(util.Arrays.equals(bytes, bytesRead))

      val oin = new ObjectInputStream(new ByteArrayInputStream(bytesRead))
      val dataBack = oin.readObject().asInstanceOf[Array[Byte]]
      assertTrue(util.Arrays.equals(data, dataBack))
   }

   def testDefaultConfiguredExpiryValues(m: Method) {
      val cacheName = "evictExpiryCache"
      var fullPathKey = "%s/rest/%s/%s".format(HOST, cacheName, m.getName)
      var post = new PostMethod(fullPathKey)
      // Live forever...
      post.setRequestHeader("timeToLiveSeconds", "-1")
      post.setRequestEntity(new StringRequestEntity("data", "text/plain", "UTF-8"))
      call(post)
      // Sleep way beyond the default in the config
      Thread.sleep(5000)
      var get = call(new GetMethod(fullPathKey))
      assertEquals("data", get.getResponseBodyAsString)
      assertNull(get.getResponseHeader("Expires"))

      var startTime = System.currentTimeMillis
      var lifespan = 3000
      fullPathKey = "%s-2".format(fullPathKey)
      post = new PostMethod(fullPathKey)
      post.setRequestHeader("Content-Type", "application/text")
      // It'll fallback on configured lifespan/maxIdle values.
      post.setRequestHeader("timeToLiveSeconds", "0")
      post.setRequestHeader("maxIdleTimeSeconds", "0")
      post.setRequestEntity(new StringRequestEntity("data2", "text/plain", "UTF-8"))
      call(post)
      while (System.currentTimeMillis < startTime + lifespan) {
         get = call(new GetMethod(fullPathKey))
         val response = get.getResponseBodyAsString
         // The entry could have expired before our request got to the server
         // Scala doesn't support break, so we need to test the current time twice
         if (System.currentTimeMillis < startTime + lifespan) {
            assertEquals("data2", response)
            assertNotNull(get.getResponseHeader("Expires"))
            Thread.sleep(100)
         }
      }

      // Make sure that in the next 20 secs data is removed
      waitNotFound(startTime, lifespan, fullPathKey)
      assertEquals(SC_NOT_FOUND, call(new GetMethod(fullPathKey)).getStatusCode)

      startTime = System.currentTimeMillis
      lifespan = 0
      fullPathKey = "%s-3".format(fullPathKey)
      post = new PostMethod(fullPathKey)
      // It will expire immediately
      post.setRequestHeader("timeToLiveSeconds", "0")
      post.setRequestEntity(new StringRequestEntity("data3", "text/plain", "UTF-8"))
      call(post)
      waitNotFound(startTime, lifespan, fullPathKey)

      fullPathKey = "%s-4".format(fullPathKey)
      post = new PostMethod(fullPathKey)
      // It will use configured maxIdle
      post.setRequestHeader("maxIdleTimeSeconds", "0")
      post.setRequestEntity(new StringRequestEntity("data4", "text/plain", "UTF-8"))
      call(post)
      // Sleep way beyond the default in the config
      Thread.sleep(2500)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, call(new GetMethod(fullPathKey)).getStatusCode)
   }

   def testPutByteArrayTwice(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      val data = Array[Byte](42, 42, 42)

      put.setRequestEntity(new ByteArrayRequestEntity(data, "application/x-java-serialized-object"))
      assertEquals(HttpServletResponse.SC_OK, call(put).getStatusCode)

      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)

      val reput = new PutMethod(fullPathKey)
      reput.setRequestEntity(new ByteArrayRequestEntity(data, "application/x-java-serialized-object"))
      assertEquals(HttpServletResponse.SC_OK, call(reput).getStatusCode)
   }

   def testDeleteSerializedObject(m: Method) {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      val data = Array[Byte](42, 42, 42)

      put.setRequestEntity(new ByteArrayRequestEntity(data, "application/x-java-serialized-object"))
      assertEquals(HttpServletResponse.SC_OK, call(put).getStatusCode)

      val get = call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)

      val delete = call(new DeleteMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, delete.getStatusCode)
   }

   private def waitNotFound(startTime: Long, lifespan: Int, fullPathKey: String) {
      if (System.currentTimeMillis < startTime + lifespan + 20000) {
         if (!SC_NOT_FOUND.equals(call(new GetMethod(fullPathKey)).getStatusCode)) {
            Thread.sleep(100)
            waitNotFound(startTime, lifespan, fullPathKey) // Good ol' tail recursion :)
         }
      }
   }

   private def put(m: Method): HttpMethodBase = put(m, "data", "application/text")

   private def put(m: Method, data: Any): HttpMethodBase = put(m, data, "application/text")

   private def put(m: Method, data: Any, contentType: String): HttpMethodBase = {
      val put = new PutMethod(fullPathKey(m))
      put.setRequestHeader("Content-Type", contentType)
      val reqEntity = data match {
         case s: String => new StringRequestEntity(s, null, null)
         case b: Array[Byte] => new InputStreamRequestEntity(new ByteArrayInputStream(b))
      }
      put.setRequestEntity(reqEntity)
      call(put)
      assertEquals(HttpServletResponse.SC_OK, put.getStatusCode)
      put
   }

   private def get(m: Method): HttpMethodBase = get(m, None)

   private def get(m: Method, unmodSince: Option[String]): HttpMethodBase =
      get(m, unmodSince, None, HttpServletResponse.SC_OK)

   private def get(m: Method, unmodSince: Option[String], acceptType: Option[String]): HttpMethodBase =
      get(m, unmodSince, acceptType, HttpServletResponse.SC_OK)

   private def get(m: Method, unmodSince: Option[String], acceptType: Option[String], expCode: Int): HttpMethodBase = {
      val get = new GetMethod(fullPathKey(m))
      if (unmodSince != None)
         get.setRequestHeader("If-Unmodified-Since", unmodSince.get)
      if (acceptType != None)
         get.setRequestHeader("Accept", acceptType.get)
      call(get)
      assertEquals(expCode, get.getStatusCode)
      get
   }

   private def fullPathKey(m: Method): String = fullPath + "/" + m.getName

   def addDay(aDate: String, days: Int): String = {
      val format = new SimpleDateFormat(DATE_PATTERN_RFC1123, Locale.US)
      val date = format.parse(aDate)
      val cal = Calendar.getInstance()
      cal.setTime(date)
      cal.add(Calendar.DATE, days)
      format.format(cal.getTime)
   }

   class ControlledCacheManager(cm: EmbeddedCacheManager,
           v2PutLatch: CountDownLatch,
           v3PutLatch: CountDownLatch,
           v2FinishLatch: CountDownLatch)
           extends AbstractDelegatingEmbeddedCacheManager(cm) {

      // DO NOT REMOVE PARENTHESES!
      override def getCache[K, V](): AdvancedCache[K, V] =
         new ControlledCache[K, V](super.getCache[K, V],
            v2PutLatch, v3PutLatch, v2FinishLatch)
   }

   class ControlledCache[String, Any](cache: Cache[String, Any],
           v2PutLatch: CountDownLatch, v3PutLatch: CountDownLatch,
           v2FinishLatch: CountDownLatch)
           extends AbstractDelegatingAdvancedCache(cache.getAdvancedCache) {
      override def replace(key: String, oldValue: Any, value: Any, metadata: Metadata): Boolean = {
         val newByteArray = value.asInstanceOf[Array[Byte]]
         val oldByteArray = oldValue.asInstanceOf[Array[Byte]]
         val oldAsString = new java.lang.String(oldByteArray)
         val newAsString = new java.lang.String(newByteArray)
         if (util.Arrays.equals(newByteArray, "data2".getBytes)) {
            log.debug("Let v3 apply...")
            v3PutLatch.countDown() // 2. Let the v3 put come in
            log.debug("Wait until v2 can be stored")
            // 3. Wait until v2 can apply
            val continue = v2PutLatch.await(10, TimeUnit.SECONDS)
            if (!continue)
               fail("Timed out waiting for v2 to be allowed")
         } else if (util.Arrays.equals(newByteArray, "data3".getBytes)) {
            log.debugf("About to store v3, let v2 apply, oldValue(for v3)=%s",
               oldAsString)
            // 4. Let data2 apply
            v2PutLatch.countDown()
            v2FinishLatch.await(10, TimeUnit.SECONDS) // Wait for data2 apply
         }
         log.debugf("Replace key=%s, oldValue=%s, value=%s",
            key, oldAsString, newAsString)

         super.replace(key, oldValue, value, metadata)
      }
   }

}

class MyNonSer {
   var name: String = "mic"

   def getName = name

   def setName(s: String) {
      name = s
   }
}

class MySer extends Serializable {
   var name: String = "mic"

   /**These are needed for Jackson to Do Its Thing */

   def getName = name

   def setName(s: String) {
      name = s
   }
}