package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT_ENCODING;
import static java.util.Collections.singletonMap;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_CSS;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_HTML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.RequestHeader.IF_MODIFIED_SINCE;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestRawClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.DateUtils;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @since 10.0
 */
@Test(groups = "functional", testName = "rest.StaticResourceTest")
public class StaticResourceTest extends AbstractRestResourceTest {

   private static final Map<String, String> NO_COMPRESSION = singletonMap(ACCEPT_ENCODING.toString(), "none");
   private RestClient noRedirectsClient;

   @BeforeClass(alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      RestClientConfigurationBuilder builder = super.getClientConfig();
      builder.followRedirects(false).addServer().host(restServer().getHost()).port(restServer().getPort());
      noRedirectsClient = RestClient.forConfiguration(builder.build());
   }

   @AfterClass
   public void afterClass() {
      super.afterClass();
      Util.close(noRedirectsClient);
   }

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
   }

   private RestResponse call(String path) {
      RestRawClient rawClient = client.raw();
      return join(rawClient.get(path, NO_COMPRESSION));
   }

   private RestResponse call(String path, String ifModifiedSince) {
      Map<String, String> allHeaders = new HashMap<>(NO_COMPRESSION);
      allHeaders.put(IF_MODIFIED_SINCE.getValue(), ifModifiedSince);
      allHeaders.putAll(NO_COMPRESSION);
      RestRawClient rawClient = client.raw();
      return join(rawClient.get(path, allHeaders));
   }

   @Override
   public Object[] factory() {
      return testFactory(StaticResourceTest.class);
   }

   @Test
   public void testGetFile() {
      RestResponse response = call("/static/nonexistent.html");
      ResponseAssertion.assertThat(response).isNotFound();

      response = call("/static");
      assertResponse(response, "static-test/index.html", "<h1>Hello</h1>", TEXT_HTML);

      response = call("/static/index.html");
      assertResponse(response, "static-test/index.html", "<h1>Hello</h1>", TEXT_HTML);

      response = call("/static/xml/file.xml");
      assertResponse(response, "static-test/xml/file.xml", "<distributed-cache", MediaType.fromString("text/xml"), APPLICATION_XML);

      response = call("/static/other/text/file.txt");
      assertResponse(response, "static-test/other/text/file.txt", "This is a text file", TEXT_PLAIN);
   }

   @Test
   public void testConsole() {
      RestResponse response1 = call("/console/page.htm");
      RestResponse response2 = call("/console/folder/test.css");
      RestResponse response3 = call("/console");

      assertResponse(response1, "static-test/console/page.htm", "console", TEXT_HTML);
      assertResponse(response2, "static-test/console/folder/test.css", ".a", TEXT_CSS);
      ResponseAssertion.assertThat(response2).isOk();

      assertResponse(response3, "static-test/console/index.html", "console", TEXT_HTML);

      RestResponse response = call("/console/");
      ResponseAssertion.assertThat(response).isOk();

      response = call("/console/create");
      ResponseAssertion.assertThat(response).isOk();

      response = call("/notconsole/");
      ResponseAssertion.assertThat(response).isNotFound();
   }

   private void assertResponse(RestResponse response, String path, String returnedText, MediaType... possibleTypes) {
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasMediaType(possibleTypes);
      ResponseAssertion.assertThat(response).containsReturnedText(returnedText);
      assertCacheHeaders(path, response);
      ResponseAssertion.assertThat(response).hasValidDate();
   }

   private void assertCacheHeaders(String path, RestResponse response) {
      int expireDuration = 60 * 60 * 24 * 31;
      File test = getTestFile(path);
      assertNotNull(test);
      ResponseAssertion.assertThat(response).hasContentLength(test.length());
      ResponseAssertion.assertThat(response).hasLastModified(test.lastModified());
      ResponseAssertion.assertThat(response).hasCacheControlHeaders("private, max-age=" + expireDuration);
      ResponseAssertion.assertThat(response).expiresAfter(expireDuration);
   }

   @Test
   public void testCacheHeaders() {
      String path = "/static/index.html";
      long lastModified = getTestFile("static-test/index.html").lastModified();


      RestResponse response = call(path, DateUtils.toRFC1123(lastModified));
      ResponseAssertion.assertThat(response).isNotModified();
      ResponseAssertion.assertThat(response).hasNoContent();

      response = call(path, "Sun, 15 Aug 1971 15:00:00 GMT");
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).containsReturnedText("<h1>Hello</h1>");

      response = call(path, DateUtils.toRFC1123(System.currentTimeMillis()));
      ResponseAssertion.assertThat(response).isNotModified();
      ResponseAssertion.assertThat(response).hasNoContent();
   }

   @Test
   public void testRedirect() {
      RestResponse response = join(noRedirectsClient.raw().get("/"));

      ResponseAssertion.assertThat(response).isRedirect();
      ResponseAssertion.assertThat(response).hasNoContent();
      assertEquals("/console/welcome", response.headers().get("Location").get(0));
   }

   private static File getTestFile(String path) {
      URL resource = StaticResourceTest.class.getClassLoader().getResource(path);
      if (resource == null) return null;
      try {
         Path p = Paths.get(resource.toURI());
         return p.toFile();
      } catch (URISyntaxException ignored) {
      }
      return null;
   }

}
