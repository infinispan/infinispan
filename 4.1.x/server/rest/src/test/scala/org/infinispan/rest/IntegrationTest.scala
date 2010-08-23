package org.infinispan.rest


import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.{Header, HttpClient}
import org.infinispan.container.entries.CacheEntry
import javax.servlet.http.{HttpServletResponse}
import org.infinispan.remoting.MIMECacheEntry
import java.io._
import org.testng.annotations.Test
import org.testng.Assert._
import java.lang.reflect.Method
import org.infinispan.manager.{CacheContainer, DefaultCacheManager}

/**
 * This tests using the Apache HTTP commons client library - but you could use anything
 * Decided to do this instead of testing the Server implementation itself, as testing the impl directly was kind of too easy.
 * (Given that RESTEasy does most of the heavy lifting !).
 *
 * @author Michael Neale
 * @author Galder Zamarreno
 * @since 4.0
 */
@Test(groups = Array("functional"), testName = "rest.IntegrationTest")
class IntegrationTest {
   val HOST = "http://localhost:8888/"
   val cacheName = CacheContainer.DEFAULT_CACHE_NAME
   val fullPath = HOST + "rest/" + cacheName
   //val HOST = "http://localhost:8080/infinispan/"

   def testBasicOperation(m: Method) = {
      // Now invoke...via HTTP
      val client = new HttpClient

      val fullPathKey = fullPath + "/" + m.getName

      val insert = new PutMethod(fullPathKey)
      val initialXML = <hey>ho</hey>

      insert.setRequestBody(new ByteArrayInputStream(initialXML.toString.getBytes))
      insert.setRequestHeader("Content-Type", "application/octet-stream")

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
      Client.call(removeAll)

      Client.call(get)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)

      val bout = new ByteArrayOutputStream
      val oo = new ObjectOutputStream(bout)
      oo.writeObject(new MIMECacheEntry("foo", "hey".getBytes))
      oo.flush
      val byteData = bout.toByteArray

      val insertMore = new PutMethod(fullPathKey)

      insertMore.setRequestBody(new ByteArrayInputStream(byteData))
      insertMore.setRequestHeader("Content-Type", "application/octet-stream")

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
         Client.call(new GetMethod(HOST + "rest/" + cacheName + "/nodata")).getStatusCode
         )
   }

   def testGet(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName      
      val post = new PostMethod(fullPathKey)
      post.setRequestHeader("Content-Type", "application/text")
      post.setRequestBody("data")
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
      post.setRequestHeader("Content-Type", "application/text")
      post.setRequestBody("data")
      Client.call(post)

      val get = Client.call(new HeadMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
      assertNotNull(get.getResponseHeader("ETag").getValue)
      assertNotNull(get.getResponseHeader("Last-Modified").getValue)
      assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)

      assertNull(get.getResponseBodyAsString)
   }

   def testPostDuplicate(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestHeader("Content-Type", "application/text")
      post.setRequestBody("data")
      Client.call(post)

      //Should get a conflict as its a DUPE post
      assertEquals(HttpServletResponse.SC_CONFLICT, Client.call(post).getStatusCode)

      val put = new PutMethod(fullPathKey)
      put.setRequestHeader("Content-Type", "application/text")
      put.setRequestBody("data")

      //Should be all ok as its a put
      assertEquals(HttpServletResponse.SC_OK, Client.call(put).getStatusCode)
   }

   def testPutDataWithTimeToLive(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val post = new PostMethod(fullPathKey)
      post.setRequestHeader("Content-Type", "application/text")
      post.setRequestHeader("timeToLiveSeconds", "2")
      post.setRequestHeader("maxIdleTimeSeconds", "3")
      post.setRequestBody("data")
      Client.call(post)

      val get = Client.call(new GetMethod(fullPathKey))
      assertEquals("data", get.getResponseBodyAsString)

      Thread.sleep(3000)
      Client.call(get)
      assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)
   }

   def testRemoveEntry(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PostMethod(fullPathKey)
      put.setRequestHeader("Content-Type", "application/text")
      put.setRequestBody("data")
      Client.call(put)

      assertEquals(HttpServletResponse.SC_OK, Client.call(new HeadMethod(fullPathKey)).getStatusCode)

      Client.call(new DeleteMethod(fullPathKey))
      assertEquals(HttpServletResponse.SC_NOT_FOUND, Client.call(new HeadMethod(fullPathKey)).getStatusCode)
   }

   def testWipeCacheBucket(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PostMethod(fullPathKey)
      put.setRequestHeader("Content-Type", "application/text")
      put.setRequestBody("data")
      Client.call(put)

      val put_ = new PostMethod(fullPathKey + "2")
      put_.setRequestHeader("Content-Type", "application/text")
      put_.setRequestBody("data")
      Client.call(put_)

      assertEquals(HttpServletResponse.SC_OK, Client.call(new HeadMethod(fullPathKey)).getStatusCode)
      Client.call(new DeleteMethod(fullPath))
      assertEquals(HttpServletResponse.SC_NOT_FOUND, Client.call(new HeadMethod(fullPathKey)).getStatusCode)
   }

   def testAsyncAddRemove(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PostMethod(fullPathKey)
      put.setRequestHeader("Content-Type", "application/text")
      put.setRequestHeader("performAsync", "true")
      put.setRequestBody("data")
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
      ManagerInstance getCache(CacheContainer.DEFAULT_CACHE_NAME) put (m.getName, obj)
      ManagerInstance getCache(CacheContainer.DEFAULT_CACHE_NAME) put (m.getName + "2", "hola")
      ManagerInstance getCache(CacheContainer.DEFAULT_CACHE_NAME) put (m.getName + "3", new MyNonSer)

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

   def testInsertSerializableObjects(m: Method) = {
      val fullPathKey = fullPath + "/" + m.getName
      val put = new PutMethod(fullPathKey)
      put.setRequestHeader("Content-Type", "application/x-java-serialized-object")
      val bout = new ByteArrayOutputStream
      new ObjectOutputStream(bout).writeObject(new MySer)
      put.setRequestBody(new ByteArrayInputStream(bout.toByteArray))
      Client.call(put)

      val x = ManagerInstance.getCache(CacheContainer.DEFAULT_CACHE_NAME).get(m.getName).asInstanceOf[MySer]
      assertTrue(x.name == "mic")
   }

}


class MyNonSer {
   var name: String = "mic"

   def getName = name

   def setName(s: String) = {name = s}
}

class MySer extends Serializable {
   var name: String = "mic"

   /**These are needed for Jackson to Do Its Thing */
   def getName = name

   def setName(s: String) = {name = s}
}