package org.infinispan.rest


import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.{Header, HttpClient}
import org.infinispan.container.entries.CacheEntry
import javax.servlet.http.{HttpServletResponse}
import org.infinispan.remoting.MIMECacheEntry
import java.io._
import org.testng.annotations.Test
import org.testng.Assert._


/**
 * This tests using the Apache HTTP commons client library - but you could use anything
 * Decided to do this instead of testing the Server implementation itself, as testing the impl directly was kind of too easy.
 * (Given that RESTEasy does most of the heavy lifting !).
 * @author Michael Neale
 */
@Test
class IntegrationTest {

  val HOST = "http://localhost:8888/"
  //val HOST = "http://localhost:8080/infinispan/"
  
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

  @Test def shouldCopeWithSerializable = {
    Client.call(new GetMethod(HOST + "rest/wang/wangKey"))

    val obj = new MySer
    obj.name = "mic"
    ManagerInstance getCache("wang") put("wangKey", obj)
    ManagerInstance getCache("wang") put("wangKey2", "hola")
    ManagerInstance getCache("wang") put("wangKey3", new MyNonSer)


    //check we can get it back as an object...
    val get = new GetMethod(HOST + "rest/wang/wangKey");
    get.setRequestHeader("Accept", "application/x-java-serialized-object")
    Client.call(get)
    assertEquals(HttpServletResponse.SC_OK, get.getStatusCode)
    val in = new ObjectInputStream(get.getResponseBodyAsStream)
    val res = in.readObject.asInstanceOf[MySer]
    assertNotNull(res)
    assertEquals("mic", res.name)
    assertEquals("application/x-java-serialized-object", get.getResponseHeader("Content-Type").getValue)

    val getStr = Client.call(new GetMethod(HOST + "rest/wang/wangKey2"))
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
    val get3 = new GetMethod(HOST + "rest/wang/wangKey3")
    get3.setRequestHeader("Accept", "application/json")
    Client.call(get3)
    assertEquals("""{"name":"mic"}""", get3.getResponseBodyAsString)
    assertEquals("application/json", get3.getResponseHeader("Content-Type").getValue)


    get3.setRequestHeader("Accept", "*/*")
    Client.call(get3)
    assertEquals(HttpServletResponse.SC_NOT_ACCEPTABLE, get3.getStatusCode)

  }


  @Test def insertSerializableObjects = {
    val put = new PutMethod(HOST + "rest/posteee/something")
    put.setRequestHeader("Content-Type", "application/x-java-serialized-object")
    val bout = new ByteArrayOutputStream
    new ObjectOutputStream(bout).writeObject(new MySer)
    put.setRequestBody(new ByteArrayInputStream(bout.toByteArray))
    Client.call(put)

    val x = ManagerInstance.getCache("posteee").get("something").asInstanceOf[MySer]
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
    /** These are needed for Jackson to Do Its Thing */
    def getName = name
    def setName(s: String) = {name = s}
}