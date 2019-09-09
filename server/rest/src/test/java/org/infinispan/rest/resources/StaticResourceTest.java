package org.infinispan.rest.resources;

import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_HTML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.eclipse.jetty.client.api.ContentResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.DateUtils;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

/**
 * @since 10.0
 */
@Test(groups = "functional", testName = "rest.StaticResourceTest")
public class StaticResourceTest extends AbstractRestResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
   }

   private ContentResponse call(String path) throws InterruptedException, ExecutionException, TimeoutException {
      String url = String.format("http://localhost:%d/rest/static/%s", restServer().getPort(), path);
      client.getContentDecoderFactories().clear();
      return client.newRequest(url).method(GET).send();
   }

   @Test
   public void testGetFile() throws InterruptedException, ExecutionException, TimeoutException {
      ContentResponse response = call("nonexistent.html");
      ResponseAssertion.assertThat(response).isNotFound();

      response = call("");
      assertResponse(response, "index.html", "<h1>Hello</h1>", TEXT_HTML);

      response = call("index.html");
      assertResponse(response, "index.html", "<h1>Hello</h1>", TEXT_HTML);

      response = call("xml/file.xml");
      assertResponse(response, "xml/file.xml", "<distributed-cache", MediaType.fromString("text/xml"), APPLICATION_XML);

      response = call("other/text/file.txt");
      assertResponse(response, "other/text/file.txt", "This is a text file", TEXT_PLAIN);
   }

   private void assertResponse(ContentResponse response, String path, String returnedText, MediaType... possibleTypes) {
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasMediaType(possibleTypes);
      ResponseAssertion.assertThat(response).containsReturnedText(returnedText);
      assertCacheHeaders(path, response);
      ResponseAssertion.assertThat(response).hasValidDate();
   }

   private void assertCacheHeaders(String path, ContentResponse response) {
      int expireDuration = 60 * 60 * 24 * 31;
      File test = getTestFile("static-test/" + path);
      assertNotNull(test);
      ResponseAssertion.assertThat(response).hasContentLength(test.length());
      ResponseAssertion.assertThat(response).hasLastModified(test.lastModified());
      ResponseAssertion.assertThat(response).hasCacheControlHeaders("private, max-age=" + expireDuration);
      ResponseAssertion.assertThat(response).expiresAfter(expireDuration);
   }

   @Test
   public void testCacheHeaders() throws InterruptedException, ExecutionException, TimeoutException {
      ContentResponse response;
      String url = String.format("http://localhost:%d/rest/static/index.html", restServer().getPort());
      long lastModified = getTestFile("static-test/index.html").lastModified();

      response = client.newRequest(url).method(GET).header("If-Modified-Since", DateUtils.toRFC1123(lastModified)).send();
      ResponseAssertion.assertThat(response).isNotModified();
      ResponseAssertion.assertThat(response).hasNoContent();

      response = client.newRequest(url).method(GET).header("If-Modified-Since", "Sun, 15 Aug 1971 15:00:00 GMT").send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).containsReturnedText("<h1>Hello</h1>");

      response = client.newRequest(url).method(GET).header("If-Modified-Since", DateUtils.toRFC1123(System.currentTimeMillis())).send();
      ResponseAssertion.assertThat(response).isNotModified();
      ResponseAssertion.assertThat(response).hasNoContent();
   }


   private static File getTestFile(String path) {
      URL resource = StaticResourceTest.class.getClassLoader().getResource(path);
      try {
         Path p = Paths.get(resource.toURI());
         return p.toFile();
      } catch (URISyntaxException ignored) {
      }
      return null;
   }

}
