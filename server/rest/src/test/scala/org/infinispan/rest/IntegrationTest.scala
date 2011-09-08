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
import org.infinispan.manager.CacheContainer
import scala.math._
import org.infinispan.test.TestingUtil
import java.text.SimpleDateFormat
import org.apache.commons.httpclient.{HttpMethodBase, Header, HttpClient}
import org.apache.commons.httpclient.HttpMethod
import java.util.{Arrays, Calendar, Locale}
import org.testng.AssertJUnit._

/**
 * This tests using the Apache HTTP commons client library - but you could use anything
 * Decided to do this instead of testing the Server implementation itself, as testing the impl directly was kind of too easy.
 * (Given that RESTEasy does most of the heavy lifting !).
 *
 * @author Michael Neale
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = Array("functional"), testName = "rest.IntegrationTest")
class IntegrationTest {
   val HOST = "http://localhost:8888"
   val cacheName = CacheContainer.DEFAULT_CACHE_NAME
   val fullPath = HOST + "/rest/" + cacheName
   val DATE_PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz";

   //val HOST = "http://localhost:8080/infinispan/"
   
   @BeforeClass
   def setUp() = {
      ServerInstance.start()
   }
   
   @AfterClass
   def tearDown() = {
      ServerInstance.stop()
   }

   def testBasicOperation(m: Method) = {
      // Now invoke...via HTTP
      val client = new HttpClient

      val fullPathKey = fullPath + "/" + m.getName

      val insert = new PutMethod(fullPathKey)
      val initialXML = <hey>ho</hey>

      insert.setRequestEntity(new ByteArrayRequestEntity(initialXML.toString.getBytes, "application/octet-stream"))      

      Client.call(insert)

      assertEquals("", insert.getResponseBodyAsString.trim)
      assertEquals(HttpServletResponse.SC_OK, insert.getStatusCode)

      val get = new GetMethod(fullPathKey)
      Client.call(get)
      val bytes = get.getResponseBody
      assertEquals(bytes.size, initialXML.toString.getBytes.size)
      assertEquals(<hey>ho</hey>.toString, get.getResponseBodyAsString)
      val hdr: Header = get.getResponseHeader("Content-Type")
      assertEquals("application/octet-stream", hdr.getValue)

      val remove = new DeleteMethod(fullPathKey);
      Client.call(remove)
      Client.call(get)

      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)

      Client.call(insert)
      Client.call(get)
      assertEquals(<hey>ho</hey>.toString, get.getResponseBodyAsString)

      val removeAll = new DeleteMethod(fullPath);
      assertEquals(HttpServletResponse.SC_OK, Client.call(removeAll).getStatusCode)

      Client.call(get)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)

      val bout = new ByteArrayOutputStream
      val oo = new ObjectOutputStream(bout)
      oo.writeObject(new MIMECacheEntry("foo", "hey".getBytes))
      oo.flush
      val byteData = bout.toByteArray

      val insertMore = new PutMethod(fullPathKey)

      insertMore.setRequestEntity(new ByteArrayRequestEntity(byteData,  "application/octet-stream"))

      Client.call(insertMore)

      val getMore = new GetMethod(fullPathKey)
      Client.call(getMore)

      val bytesBack = getMore.getResponseBody
      assertEquals(byteData.length, bytesBack.length)

      val oin = new ObjectInputStream(new ByteArrayInputStream(bytesBack))
      val ce = oin.readObject.asInstanceOf[MIMECacheEntry]
      assertEquals("foo", ce.contentType)
   }

   def testEmptyGet = {
      assertEquals(
         HttpServletResponse.SC_NOT_FOUND,
         Client.call(new GetMethod(HOST + "/rest/" + cacheName + "/nodata")).getStatusCode
      )
   }

   def testGet(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      Client.call(post)

      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      assertNotNull(get.getResponseHeader("ETag").getValue)
      assertNotNull(get.getResponseHeader("Last-Modified").getValue)
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)
      assertEquals("data", get.getResponseBodyAsString)
   }

   def testHead(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      Client.call(post)

      val get = Client.call(new HeadMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      assertNotNull(get.getResponseHeader("ETag").getValue)
      assertNotNull(get.getResponseHeader("Last-Modified").getValue)
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)

      assertNull(get.getResponseBodyAsString)
   }

   def testGetIfUnmodified(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestEntity(new StringRequestEntity("data", "application/text", null))
      Client.call(post)

      var get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      assertNotNull(get.getResponseHeader("ETag").getValue)
      val lastMod = get.getResponseHeader("Last-Modified").getValue
      assertNotNull(lastMod)
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)
      assertEquals("data", get.getResponseBodyAsString)

      // now get again
      val getAgain = new GetMethod(fullPathKey)
      getAgain.addRequestHeader("If-Unmodified-Since", lastMod)
      get = Client.call(getAgain)
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      assertNotNull(get.getResponseHeader("ETag").getValue)
      assertNotNull(get.getResponseHeader("Last-Modified").getValue)
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)
      assertEquals("data", get.getResponseBodyAsString)
   }

   def testPostDuplicate(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(post)

      //Should get a conflict as its a DUPE post
      assertEquals(HttpServletResponse.SC_CONFLICT, Client.call(post).getStatusCode)

      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))

      //Should be all ok as its a put
      assertEquals(HttpServletResponse.SC_OK, Client.call(put).getStatusCode)
   }

   def testPutDataWithTimeToLive(m: Method) = putAndAssertEphemeralData(m, "2", "3")

   def testPutDataWithMaxIdleOnly(m: Method) = putAndAssertEphemeralData(m, "", "3")

   def testPutDataWithTimeToLiveOnly(m: Method) = putAndAssertEphemeralData(m, "3", "")

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
      Client.call(post)

      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals("data", get.getResponseBodyAsString)

      TestingUtil.sleepThread((maxWaitTime + 1) * 1000)
      Client.call(get)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)
   }
   
   def testPutDataWithIfMatch(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)
      
      // Now get it to retrieve some attributes
      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val etag = get.getResponseHeader("ETag").getValue      
      
      // Put again using the If-Match with the ETag we got back from the get
      val reput = new PutMethod(fullPathKey)      
      reput.setRequestHeader("If-Match", etag);
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_OK, Client.call(reput).getStatusCode)
      
      // Try to put again, but with a different ETag
      val reputAgain = new PutMethod(fullPathKey)
      reputAgain.setRequestHeader("If-Match", "x");
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, Client.call(reputAgain).getStatusCode)
   }
   
   def testPutDataWithIfNoneMatch(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)
      
      // Now get it to retrieve some attributes
      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val etag = get.getResponseHeader("ETag").getValue      
      
      // Put again using the If-Match with the ETag we got back from the get
      val reput = new PutMethod(fullPathKey)      
      reput.setRequestHeader("If-None-Match", "x");
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_OK, Client.call(reput).getStatusCode)
      
      // Try to put again, but with a different ETag
      val reputAgain = new PutMethod(fullPathKey)      
      reputAgain.setRequestHeader("If-None-Match", etag);
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, Client.call(reputAgain).getStatusCode)
   }
   
   def testPutDataWithIfModifiedSince(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)
      
      // Now get it to retrieve some attributes
      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)      
      val lastMod = get.getResponseHeader("Last-Modified").getValue      
      
      // Put again using the If-Modified-Since with the lastMod we got back from the get
      val reput = new PutMethod(fullPathKey)
      reput.setRequestHeader("If-Modified-Since", lastMod)
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_NOT_MODIFIED, Client.call(reput).getStatusCode)
      
      // Try to put again, but with an older last modification date
      val reputAgain = new PutMethod(fullPathKey)
      val dateMinus = addDay(lastMod, -1)      
      reputAgain.setRequestHeader("If-Modified-Since", dateMinus)
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_OK, Client.call(reputAgain).getStatusCode)
   }
   
   def testPutDataWithIfUnModifiedSince(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)
      
      // Now get it to retrieve some attributes
      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)      
      val lastMod = get.getResponseHeader("Last-Modified").getValue
      
      // Put again using the If-Unmodified-Since with a date earlier than the one we got back from the GET
      val reput = new PutMethod(fullPathKey)
      val dateMinus = addDay(lastMod, -1)      
      reput.setRequestHeader("If-Unmodified-Since", dateMinus)      
      reput.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, Client.call(reput).getStatusCode)
      
      // Try to put again, but using the date returned by the GET
      val reputAgain = new PutMethod(fullPathKey)
      reputAgain.setRequestHeader("If-Unmodified-Since", lastMod);
      reputAgain.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      assertEquals(HttpServletResponse.SC_OK, Client.call(reputAgain).getStatusCode)
   }
   
   def testDeleteDataWithIfMatch(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)
      
      // Now get it to retrieve some attributes
      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val etag = get.getResponseHeader("ETag").getValue      
      
      // Attempt to delete with a wrong ETag
      val delete = new DeleteMethod(fullPathKey)      
      delete.setRequestHeader("If-Match", "x");    
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, Client.call(delete).getStatusCode)
      
      // Try to delete again, but with the proper ETag
      val deleteAgain = new DeleteMethod(fullPathKey)
      deleteAgain.setRequestHeader("If-Match", etag);      
      assertEquals(HttpServletResponse.SC_OK, Client.call(deleteAgain).getStatusCode)
   }
   
   def testDeleteDataWithIfNoneMatch(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)
      
      // Now get it to retrieve some attributes
      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val etag = get.getResponseHeader("ETag").getValue      
      
      // Attempt to delete with the ETag
      val delete = new DeleteMethod(fullPathKey)      
      delete.setRequestHeader("If-None-Match", etag);    
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, Client.call(delete).getStatusCode)
      
      // Try to delete again, but with a non-matching ETag
      val deleteAgain = new DeleteMethod(fullPathKey)
      deleteAgain.setRequestHeader("If-None-Match", "x");      
      assertEquals(HttpServletResponse.SC_OK, Client.call(deleteAgain).getStatusCode)
   }
   
   def testDeleteDataWithIfModifiedSince(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)
      
      // Now get it to retrieve some attributes
      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)      
      val lastMod = get.getResponseHeader("Last-Modified").getValue      
      
      // Attempt to delete using the If-Modified-Since header with the lastMod we got back from the get
      val delete = new DeleteMethod(fullPathKey)
      delete.setRequestHeader("If-Modified-Since", lastMod)      
      assertEquals(HttpServletResponse.SC_NOT_MODIFIED, Client.call(delete).getStatusCode)
      
      // Try to delete again, but with an older last modification date
      val deleteAgain = new DeleteMethod(fullPathKey)      
      val dateMinus = addDay(lastMod, -1)      
      deleteAgain.setRequestHeader("If-Modified-Since", dateMinus)      
      assertEquals(HttpServletResponse.SC_OK, Client.call(deleteAgain).getStatusCode)
   }
   
   def testDeleteDataWithIfUnmodifiedSince(m: Method) {
      // Put the data first
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)
      
      // Now get it to retrieve some attributes
      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)      
      val lastMod = get.getResponseHeader("Last-Modified").getValue      
      
      // Attempt to delete using the If-Unmodified-Since header with a date earlier than the one we got back from the GET
      val delete = new DeleteMethod(fullPathKey)
      val dateMinus = addDay(lastMod, -1)      
      delete.setRequestHeader("If-Unmodified-Since", dateMinus)      
      assertEquals(HttpServletResponse.SC_PRECONDITION_FAILED, Client.call(delete).getStatusCode)
      
      // Try to delete again, but with an older last modification date
      val deleteAgain = new DeleteMethod(fullPathKey)            
      deleteAgain.setRequestHeader("If-Unmodified-Since", lastMod)      
      assertEquals(HttpServletResponse.SC_OK, Client.call(deleteAgain).getStatusCode)
   }
   
   def testDeleteCachePreconditionUnimplemented(m: Method) =
      testDeletePreconditionalUnimplemented(fullPath)

   private def testDeletePreconditionalUnimplemented(fullPathKey: String) {
     testDeletePreconditionalUnimplemented(fullPathKey, "If-Match")
     testDeletePreconditionalUnimplemented(fullPathKey, "If-None-Match")
     testDeletePreconditionalUnimplemented(fullPathKey, "If-Modified-Since")
     testDeletePreconditionalUnimplemented(fullPathKey, "If-Unmodified-Since")
   }
   
   private def testDeletePreconditionalUnimplemented(
         fullPathKey: String, preconditionalHeaderName: String) = {
      val delete = new DeleteMethod(fullPathKey)
      delete.setRequestHeader(preconditionalHeaderName, "*");
      Client.call(delete)
      
      assertNotImplemented(delete)
   }
   
   private def assertNotImplemented(method: HttpMethod) = {
      assertEquals(method.getStatusCode, 501)
      assertEquals(method.getStatusText, "Not Implemented")
      assert(method.getResponseBodyAsString.toLowerCase().contains("precondition"))
   }

   def testRemoveEntry(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PostMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)

      assertEquals(HttpServletResponse.SC_OK, Client.call(new HeadMethod(fullPathKey)).getStatusCode)

      Client.call(new DeleteMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_NOT_FOUND, Client.call(new HeadMethod(fullPathKey)).getStatusCode)
   }

   def testWipeCacheBucket(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PostMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)

      val put_ = new PostMethod(fullPathKey + "2")
      put_.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put_)

      assertEquals(HttpServletResponse.SC_OK, Client.call(new HeadMethod(fullPathKey)).getStatusCode)
      Client.call(new DeleteMethod(fullPath))
      assertEquals(HttpServletResponse.SC_NOT_FOUND, Client.call(new HeadMethod(fullPathKey)).getStatusCode)
   }

   def testAsyncAddRemove(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PostMethod(fullPathKey)      
      put.setRequestHeader("performAsync", "true")
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client.call(put)

      Thread.sleep(50)
      assertEquals(HttpServletResponse.SC_OK, Client.call(new HeadMethod(fullPathKey)).getStatusCode)

      val del = new DeleteMethod(fullPathKey);
      del.setRequestHeader("performAsync", "true")
      Client.call(del)
      Thread.sleep(50)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, Client.call(new HeadMethod(fullPathKey)).getStatusCode)
   }

   def testShouldCopeWithSerializable(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      Client.call(new GetMethod(fullPathKey))

      val obj = new MySer
      obj.name = "mic"
      ManagerInstance getCache (CacheContainer.DEFAULT_CACHE_NAME) put (m.getName, obj)
      ManagerInstance getCache (CacheContainer.DEFAULT_CACHE_NAME) put (m.getName + "2", "hola")
      ManagerInstance getCache (CacheContainer.DEFAULT_CACHE_NAME) put (m.getName + "3", new MyNonSer)

      //check we can get it back as an object...
      val get = new GetMethod(fullPathKey);
      get.setRequestHeader("Accept", "application/x-java-serialized-object")
      Client.call(get)
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      val in = new ObjectInputStream(get.getResponseBodyAsStream)
      val res = in.readObject.asInstanceOf[MySer]
      assertNotNull(res)
      assertEquals("mic", res.name)
      assertEquals("application/x-java-serialized-object", get.getResponseHeader("Content-Type").getValue)

      val getStr = Client.call(new GetMethod(fullPathKey + "2"))
      assertEquals("hola", getStr.getResponseBodyAsString)
      assertEquals("text/plain", getStr.getResponseHeader("Content-Type").getValue)

      //now check we can get it back as JSON if we want...
      get.setRequestHeader("Accept", "application/json")
      Client.call(get)
      assertEquals("""{"name":"mic"}""", get.getResponseBodyAsString)
      assertEquals("application/json", get.getResponseHeader("Content-Type").getValue)

      //and why not XML
      get.setRequestHeader("Accept", "application/xml")
      Client.call(get)
      assertEquals("application/xml", get.getResponseHeader("Content-Type").getValue)
      assertTrue(get.getResponseBodyAsString.indexOf("<org.infinispan.rest.MySer>") > -1)

      //now check we can get it back as JSON if we want...
      val get3 = new GetMethod(fullPathKey + "3")
      get3.setRequestHeader("Accept", "application/json")
      Client.call(get3)
      assertEquals("""{"name":"mic"}""", get3.getResponseBodyAsString)
      assertEquals("application/json", get3.getResponseHeader("Content-Type").getValue)
   }

   def testInsertSerializableObjects(m: Method) {
      val bout = new ByteArrayOutputStream
      new ObjectOutputStream(bout).writeObject(new MySer)
      put(m, bout.toByteArray, "application/x-java-serialized-object")
      ManagerInstance.getCache(CacheContainer.DEFAULT_CACHE_NAME)
              .get(m.getName).asInstanceOf[Array[Byte]]
   }

   def testNonexistentCache(m: Method) = {
      val fullPathKey = HOST + "/rest/nonexistent/" + m.getName
      val get = new GetMethod(fullPathKey)
      Client call get
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)

      val head = Client call new HeadMethod(fullPathKey)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, head.getStatusCode)

      val put = new PostMethod(fullPathKey)
      put.setRequestEntity(new StringRequestEntity("data", "application/text", "UTF-8"))
      Client call put
      assertEquals(HttpServletResponse.SC_NOT_FOUND, put.getStatusCode)
   }

   def testByteArrayAsSerializedObjects(m: Method) =
      sendByteArrayAs(m, "application/x-java-serialized-object")

   def testByteArrayAsOctecStreamObjects(m: Method) =
      sendByteArrayAs(m, "application/octet-stream")

   private def sendByteArrayAs(m: Method, contentType: String) {
      val serializedOnClient: Array[Byte] = Array(0x65, 0x66, 0x67)
      put(m, serializedOnClient, contentType)
      val dataRead = new BufferedInputStream(
         get(m, None, Some(contentType)).getResponseBodyAsStream)
      val bytesRead = new Array[Byte](3)
      dataRead.read(bytesRead)
      assertEquals(serializedOnClient, bytesRead)
   }

   def testIfUnmodifiedSince(m: Method) = {
      put(m)
      var result = get(m)
      val dateLast = result.getResponseHeader("Last-Modified").getValue()
      val dateMinus = addDay(dateLast, -1)
      val datePlus = addDay(dateLast, 1)
      assertNotNull(get(m, Some(dateLast)).getResponseBodyAsString())
      assertNotNull(get(m, Some(datePlus)).getResponseBodyAsString())
      result = get(m, Some(dateMinus), None, HttpServletResponse.SC_PRECONDITION_FAILED)
   }

   def testETagChanges(m: Method) = {
      put(m, "data1")
      val eTagFirst = get(m).getResponseHeader("ETag").getValue()
      // second get should get the same ETag
      assertEquals(eTagFirst, get(m).getResponseHeader("ETag").getValue())
      // do second PUT
      put(m, "data2")
      // get ETag again
      val eTagSecond = get(m).getResponseHeader("ETag").getValue()
      // println("etag1 %s; etag2 %s; equals? %b".format(eTagFirst, eTagSecond, eTagFirst.equals(eTagSecond)))
      assertFalse(eTagFirst.equals(eTagSecond))
   }

   def testSerializedStringGetBytes(m: Method) {
      val data = ("v-" + m.getName).getBytes("UTF-8")

      val bout = new ByteArrayOutputStream()
      val oo = new ObjectOutputStream(bout)
      oo.writeObject(data)
      oo.flush()

      val bytes = bout.toByteArray()
      put(m, bytes, "application/x-java-serialized-object")

      val bytesRead = get(m, None, Some("application/x-java-serialized-object")).getResponseBody()
      assertTrue(Arrays.equals(bytes, bytesRead))

      val oin = new ObjectInputStream(new ByteArrayInputStream(bytesRead))
      val dataBack = oin.readObject().asInstanceOf[Array[Byte]]
      assertTrue(Arrays.equals(data, dataBack))
   }

   def testDefaultConfiguredExpiryValues(m: Method) {
      val cacheName = "evictExpiryCache"
      var fullPathKey = "%s/rest/%s/%s".format(HOST, cacheName, m.getName)
      var post = new PostMethod(fullPathKey)      
      // Live forever...
      post.setRequestHeader("timeToLiveSeconds", "-1")
      post.setRequestEntity(new StringRequestEntity("data", "text/plain", "UTF-8"))
      Client.call(post)
      // Sleep way beyond the default in the config
      Thread.sleep(5000)
      assertEquals("data", Client.call(new GetMethod(fullPathKey)).getResponseBodyAsString)

      var startTime = System.currentTimeMillis
      var lifespan = 3000
      fullPathKey = "%s-2".format(fullPathKey)
      post = new PostMethod(fullPathKey)
      post.setRequestHeader("Content-Type", "application/text")
      // It'll fallback on configured lifespan/maxIdle values.
      post.setRequestHeader("timeToLiveSeconds", "0")
      post.setRequestHeader("maxIdleTimeSeconds", "0")
      post.setRequestEntity(new StringRequestEntity("data2", "text/plain", "UTF-8"))
      Client.call(post)
      while (System.currentTimeMillis() < startTime + lifespan - 10) {
         assertEquals("data2", Client.call(new GetMethod(fullPathKey)).getResponseBodyAsString)
         Thread.sleep(100)
      }
      // Make sure that in the next 20 secs data is removed
      waitNotFound(startTime, lifespan, fullPathKey)
      assertEquals(SC_NOT_FOUND, Client.call(new GetMethod(fullPathKey)).getStatusCode)

      startTime = System.currentTimeMillis
      lifespan = 0
      fullPathKey = "%s-3".format(fullPathKey)
      post = new PostMethod(fullPathKey)      
      // It will expire immediately
      post.setRequestHeader("timeToLiveSeconds", "0")
      post.setRequestEntity(new StringRequestEntity("data3", "text/plain", "UTF-8"))
      Client.call(post)
      waitNotFound(startTime, lifespan, fullPathKey)

      fullPathKey = "%s-4".format(fullPathKey)
      post = new PostMethod(fullPathKey)      
      // It will use configured maxIdle
      post.setRequestHeader("maxIdleTimeSeconds", "0")
      post.setRequestEntity(new StringRequestEntity("data4", "text/plain", "UTF-8"))
      Client.call(post)
      // Sleep way beyond the default in the config
      Thread.sleep(2500)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, Client.call(new GetMethod(fullPathKey)).getStatusCode)
   }

   private def waitNotFound(startTime: Long, lifespan: Int, fullPathKey: String) {
      if (System.currentTimeMillis < startTime + lifespan + 20000) {
         if (!SC_NOT_FOUND.equals(Client.call(new GetMethod(fullPathKey)).getStatusCode)) {
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
         case s: String => new StringRequestEntity(s)
         case b: Array[Byte] => new InputStreamRequestEntity(new ByteArrayInputStream(b))
      }
      put.setRequestEntity(reqEntity)
      Client call put
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
      Client call get
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
      format.format(cal.getTime())
   }
}


class MyNonSer {
   var name: String = "mic"

   def getName = name

   def setName(s: String) = {
      name = s
   }
}

class MySer extends Serializable {
   var name: String = "mic"

   /**These are needed for Jackson to Do Its Thing */

   def getName = name

   def setName(s: String) = {
      name = s
   }
}
