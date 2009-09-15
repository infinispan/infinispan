package org.infinispan.rest


import apache.commons.httpclient.methods._
import apache.commons.httpclient.{Header, HttpClient}
import container.entries.CacheEntry
import remoting.MIMECacheEntry
import java.io._
import javax.servlet.http.HttpServletResponse
import junit.framework.TestCase
import junit.framework.Assert._


/**
 * This tests using the Apache HTTP commons client library - but you could use anything
 * Decided to do this instead of testing the Server implementation itself, as testing the impl directly was kind of too easy.
 * (Given that RESTEasy does most of the heavy lifting !).
 * @author Michael Neale
 */
class IntegrationTest extends TestCase {

  val HOST = "http://localhost:8888/"
  //val HOST = "http://localhost:8080/infinispan-rest/"
  
  def testBasicOperation = {

    //now invoke...via HTTP
    val client = new HttpClient

    val insert = new PutMethod(HOST + "rest/mycache/mydata")
    val initialXML = <hey>ho</hey>

    insert.setRequestBody(new ByteArrayInputStream(initialXML.toString.getBytes))
    insert.setRequestHeader("Content-Type", "application/octet-stream")

    Client.call(insert)

    assertEquals("", insert.getResponseBodyAsString.trim)
    assertEquals(HttpServletResponse.SC_OK, insert.getStatusCode)

    val get = new GetMethod(HOST + "rest/mycache/mydata")
    Client.call(get)
    val bytes = get.getResponseBody
    assertEquals(bytes.size, initialXML.toString.getBytes.size)
    assertEquals(<hey>ho</hey>.toString, get.getResponseBodyAsString)
    val hdr: Header = get.getResponseHeader("Content-Type")
    assertEquals("application/octet-stream", hdr.getValue)

    val remove = new DeleteMethod(HOST + "rest/mycache/mydata");
    Client.call(remove)
    Client.call(get)

    assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)

    Client.call(insert)
    Client.call(get)
    assertEquals(<hey>ho</hey>.toString, get.getResponseBodyAsString)

    val removeAll = new DeleteMethod(HOST + "rest/mycache");
    Client.call(removeAll)

    Client.call(get)
    assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)

    val bout = new ByteArrayOutputStream
    val oo = new ObjectOutputStream(bout)
    oo.writeObject(new MIMECacheEntry("foo", "hey".getBytes))
    oo.flush
    val byteData = bout.toByteArray

    val insertMore = new PutMethod(HOST + "rest/mycache/mydata")

    insertMore.setRequestBody(new ByteArrayInputStream(byteData))
    insertMore.setRequestHeader("Content-Type", "application/octet-stream")

    Client.call(insertMore)

    val getMore = new GetMethod(HOST + "rest/mycache/mydata")
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
      Client.call(new GetMethod(HOST + "rest/emptycache/nodata")).getStatusCode
      )
  }

  def testGet = {
    val post = new PostMethod(HOST + "rest/more2/data")
    post.setRequestHeader("Content-Type", "application/text")
    post.setRequestBody("data")
    Client.call(post)

    val get = Client.call(new GetMethod(HOST + "rest/more2/data"))
    assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
    assertNotNull(get.getResponseHeader("ETag").getValue)
    assertNotNull(get.getResponseHeader("Last-Modified").getValue)
    assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)
    assertEquals("data", get.getResponseBodyAsString)


  }


  def testHead = {
    val post = new PostMethod(HOST + "rest/more/data")
    post.setRequestHeader("Content-Type", "application/text")
    post.setRequestBody("data")
    Client.call(post)

    val get = Client.call(new HeadMethod(HOST + "rest/more/data"))
    assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
    assertNotNull(get.getResponseHeader("ETag").getValue)
    assertNotNull(get.getResponseHeader("Last-Modified").getValue)
    assertEquals("application/text", get.getResponseHeader("Content-Type").getValue)

    assertNull(get.getResponseBodyAsString)
  }

  def testPostDuplicate() = {
    val post = new PostMethod(HOST + "rest/posteee/data")
    post.setRequestHeader("Content-Type", "application/text")
    post.setRequestBody("data")
    Client.call(post)

    //Should get a conflict as its a DUPE post
    assertEquals(HttpServletResponse.SC_CONFLICT, Client.call(post).getStatusCode)

    val put = new PutMethod(HOST + "rest/posteee/data")
    put.setRequestHeader("Content-Type", "application/text")
    put.setRequestBody("data")

    //Should be all ok as its a put
    assertEquals(HttpServletResponse.SC_OK, Client.call(put).getStatusCode)

  }

  def testPutDataWithTimeToLive = {
    val post = new PostMethod(HOST + "rest/putttl/data")
    post.setRequestHeader("Content-Type", "application/text")
    post.setRequestHeader("timeToLiveSeconds", "2")
    post.setRequestHeader("maxIdleTimeSeconds", "3")
    post.setRequestBody("data")
    Client.call(post)

    val get = Client.call(new GetMethod(HOST + "rest/putttl/data"))
    assertEquals("data", get.getResponseBodyAsString)

    Thread.sleep(3000)
    Client.call(get)
    assertEquals(HttpServletResponse.SC_NOT_FOUND, get.getStatusCode)
  }

  def testRemoveEntry = {
    val put = new PostMethod(HOST + "rest/posteee/toremove")
    put.setRequestHeader("Content-Type", "application/text")
    put.setRequestBody("data")
    Client.call(put)

    assertEquals(HttpServletResponse.SC_OK, Client.call(new HeadMethod(HOST + "rest/posteee/toremove")).getStatusCode)

    Client.call(new DeleteMethod(HOST + "rest/posteee/toremove"))
    assertEquals(HttpServletResponse.SC_NOT_FOUND, Client.call(new HeadMethod(HOST + "rest/posteee/toremove")).getStatusCode)

  }

  def testWipeCacheBucket = {
    val put = new PostMethod(HOST + "rest/posteee/toremove")
    put.setRequestHeader("Content-Type", "application/text")
    put.setRequestBody("data")
    Client.call(put)

    val put_ = new PostMethod(HOST + "rest/posteee/toremove2")
    put_.setRequestHeader("Content-Type", "application/text")
    put_.setRequestBody("data")
    Client.call(put_)

    assertEquals(HttpServletResponse.SC_OK, Client.call(new HeadMethod(HOST + "rest/posteee/toremove")).getStatusCode)
    Client.call(new DeleteMethod(HOST + "rest/posteee"))
    assertEquals(HttpServletResponse.SC_NOT_FOUND, Client.call(new HeadMethod(HOST + "rest/posteee/toremove")).getStatusCode)
  }

  def testAsyncAddRemove = {
    val put = new PostMethod(HOST + "rest/posteee/async")
    put.setRequestHeader("Content-Type", "application/text")
    put.setRequestHeader("performAsync", "true")
    put.setRequestBody("data")
    Client.call(put)

    Thread.sleep(50)
    assertEquals(HttpServletResponse.SC_OK, Client.call(new HeadMethod(HOST + "rest/posteee/async")).getStatusCode)

    val del = new DeleteMethod(HOST + "rest/posteee/async");
    del.setRequestHeader("performAsync", "true")
    Client.call(del)
    Thread.sleep(50)
    assertEquals(HttpServletResponse.SC_NOT_FOUND, Client.call(new HeadMethod(HOST + "rest/posteee/async")).getStatusCode)
  }











}