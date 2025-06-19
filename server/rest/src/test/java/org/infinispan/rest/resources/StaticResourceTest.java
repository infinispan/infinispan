package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT_ENCODING;
import static java.util.Collections.singletonMap;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JS;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_CSS;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_HTML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.RequestHeader.IF_MODIFIED_SINCE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
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
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.rest.DateUtils;
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
   private String CONSOLE_DEFAULT = "index.html";
   private String CONSOLE_CONFIG_DEFAULT = "config.js";

   @BeforeClass(alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      RestClientConfigurationBuilder builder = super.getClientConfig("admin", "admin");
      builder.followRedirects(false).addServer().host(restServer().getHost()).port(restServer().getPort());
      noRedirectsClient = RestClient.forConfiguration(builder.build());
   }

   @AfterClass
   public void afterClass() {
      super.afterClass();
      Util.close(noRedirectsClient);
   }

   private RestResponse call(String path) {
      return join(client.raw().get(path, NO_COMPRESSION));
   }

   private RestResponse call(String path, String ifModifiedSince) {
      Map<String, String> allHeaders = new HashMap<>(NO_COMPRESSION);
      allHeaders.put(IF_MODIFIED_SINCE.toString(), ifModifiedSince);
      allHeaders.putAll(NO_COMPRESSION);
      return join(client.raw().get(path, allHeaders));
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new StaticResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(false),
            new StaticResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(true),
            new StaticResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(false),
            new StaticResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(true),
            new StaticResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(false),
            new StaticResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(true),
            new StaticResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(false),
            new StaticResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(true),
      };
   }

   @Test
   public void testGetFile() {
      RestResponse response = call("/static/nonexistent.html");
      assertThat(response).isNotFound();

      response = call("/static");
      assertResponse(response, "static-test/index.html", "/console/", TEXT_HTML);

      response = call("/static/index.html");
      assertResponse(response, "static-test/index.html", "/console/", TEXT_HTML);

      response = call("/static/config.js");
      assertResponse(response, "static-test/config.js", "/rest", APPLICATION_JS);

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
      RestResponse response4 = call("/console/cache/people");
      RestResponse response5 = call("/console/cache/peo.ple");

      assertResponse(response1, "static-test/console/page.htm", "console", TEXT_HTML);
      assertResponse(response2, "static-test/console/folder/test.css", ".a", TEXT_CSS);
      assertThat(response2).isOk();

      assertResponse(response3, "static-test/console/index.html", "console", TEXT_HTML);
      assertThat(response4).isOk();
      assertThat(response5).isOk();

      RestResponse response = call("/console/");
      assertThat(response).isOk();

      response = call("/console/create");
      assertThat(response).isOk();

      response = call("/notconsole/");
      assertThat(response).isNotFound();
   }

   private void assertResponse(RestResponse response, String path, String returnedText, MediaType... possibleTypes) {
      assertThat(response).isOk();
      assertThat(response).hasMediaType(possibleTypes);
      assertThat(response).containsReturnedText(returnedText);
      assertCacheHeaders(path, response);
      assertThat(response).hasValidDate();
   }

   private void assertCacheHeaders(String path, RestResponse response) {
      int expireDuration = 60 * 60 * 24 * 31;
      File test = getTestFile(path);
      assertNotNull(test);
      // Console Default and Console config are served as String
      if (protocol == HTTP_11 && !path.contains(CONSOLE_DEFAULT) && !path.contains(CONSOLE_CONFIG_DEFAULT)) {
         assertThat(response).hasTransferEncoding("chunked");
      } else {
         assertThat(response).hasNotTransferEncoding();
      }
      assertThat(response).hasLastModified(test.lastModified());
      assertThat(response).hasCacheControlHeaders("private, max-age=" + expireDuration);
      assertThat(response).expiresAfter(expireDuration);
   }

   @Test
   public void testCacheHeaders() {
      String path = "/static/index.html";
      long lastModified = getTestFile("static-test/index.html").lastModified();


      RestResponse response = call(path, DateUtils.toRFC1123(lastModified));
      assertThat(response).isNotModified();
      assertThat(response).hasNoContent();

      response = call(path, "Sun, 15 Aug 1971 15:00:00 GMT");
      assertThat(response).isOk();
      assertThat(response).containsReturnedText("/console/");

      response = call(path, DateUtils.toRFC1123(System.currentTimeMillis()));
      assertThat(response).isNotModified();
      assertThat(response).hasNoContent();
   }

   @Test
   public void testRedirect() {
      RestResponse response = join(noRedirectsClient.raw().get("/"));

      assertThat(response).isRedirect();
      assertThat(response).hasNoContent();
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
