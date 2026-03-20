package org.infinispan.rest.framework.impl;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Schema;
import org.testng.annotations.Test;

import io.netty.handler.codec.http.HttpHeaderNames;

@Test(groups = "unit", testName = "rest.framework.impl.ValidatingRestRequestTest")
public class ValidatingRestRequestTest {

   @Test
   public void testDeclaredQueryParameterAllowed() {
      Invocation invocation = invocation(Method.GET, "/v3/caches/{cacheName}")
            .parameter("pretty", ParameterIn.QUERY, false, Schema.BOOLEAN, "Pretty print")
            .create().iterator().next();

      ValidatingRestRequest validating = wrap(Method.GET, "/v3/caches/myCache", invocation);

      assertThatNoException().isThrownBy(() -> validating.getParameter("pretty"));
   }

   @Test
   public void testUndeclaredQueryParameterThrows() {
      Invocation invocation = invocation(Method.GET, "/v3/caches/{cacheName}").create().iterator().next();

      ValidatingRestRequest validating = wrap(Method.GET, "/v3/caches/myCache", invocation);

      assertThatThrownBy(() -> validating.getParameter("undeclared"))
            .isInstanceOf(UndeclaredParameterException.class)
            .hasMessageContaining("Query parameter")
            .hasMessageContaining("undeclared");
   }

   @Test
   public void testDeclaredPathVariableAllowed() {
      Invocation invocation = invocation(Method.GET, "/v3/caches/{cacheName}")
            .parameter("cacheName", ParameterIn.PATH, true, Schema.STRING, "The cache name")
            .create().iterator().next();

      RestRequest delegate = request(Method.GET, "/v3/caches/myCache");
      delegate.setVariables(Map.of("cacheName", "myCache"));
      ValidatingRestRequest validating = new ValidatingRestRequest(delegate, invocation);

      assertThatNoException().isThrownBy(() -> validating.variables().get("cacheName"));
   }

   @Test
   public void testUndeclaredPathVariableThrows() {
      Invocation invocation = invocation(Method.GET, "/v3/caches/{cacheName}").create().iterator().next();

      RestRequest delegate = request(Method.GET, "/v3/caches/myCache");
      delegate.setVariables(Map.of("cacheName", "myCache"));
      ValidatingRestRequest validating = new ValidatingRestRequest(delegate, invocation);

      assertThatThrownBy(() -> validating.variables().get("cacheName"))
            .isInstanceOf(UndeclaredParameterException.class)
            .hasMessageContaining("Path variable")
            .hasMessageContaining("cacheName");
   }

   @Test
   public void testUndeclaredSpecializedHeaderThrows() {
      Invocation invocation = invocation(Method.GET, "/v3/caches/{cacheName}").create().iterator().next();

      ValidatingRestRequest validating = wrap(Method.GET, "/v3/caches/myCache", invocation);

      assertThatThrownBy(validating::getTimeToLiveSecondsHeader)
            .isInstanceOf(UndeclaredParameterException.class)
            .hasMessageContaining("Header")
            .hasMessageContaining(RequestHeader.TTL_SECONDS_HEADER.toString());
   }

   @Test
   public void testDeclaredSpecializedHeaderAllowed() {
      Invocation invocation = invocation(Method.PUT, "/v3/caches/{cacheName}/entries/{cacheKey}")
            .parameter(RequestHeader.TTL_SECONDS_HEADER, ParameterIn.HEADER, false, Schema.INTEGER, "TTL")
            .parameter(RequestHeader.MAX_TIME_IDLE_HEADER, ParameterIn.HEADER, false, Schema.INTEGER, "Max idle")
            .parameter(RequestHeader.CREATED_HEADER, ParameterIn.HEADER, false, Schema.LONG, "Created")
            .parameter(RequestHeader.LAST_USED_HEADER, ParameterIn.HEADER, false, Schema.LONG, "Last used")
            .parameter(RequestHeader.FLAGS_HEADER, ParameterIn.HEADER, false, Schema.STRING, "Flags")
            .create().iterator().next();

      ValidatingRestRequest validating = wrap(Method.PUT, "/v3/caches/myCache/entries/key1", invocation);

      assertThatNoException().isThrownBy(validating::getTimeToLiveSecondsHeader);
      assertThatNoException().isThrownBy(validating::getMaxIdleTimeSecondsHeader);
      assertThatNoException().isThrownBy(validating::getCreatedHeader);
      assertThatNoException().isThrownBy(validating::getLastUsedHeader);
      assertThatNoException().isThrownBy(validating::getFlags);
      assertThatNoException().isThrownBy(validating::getAdminFlags);
   }

   @Test
   public void testDeclaredHttpHeaderNamesAllowed() {
      Invocation invocation = invocation(Method.GET, "/v3/caches/{cacheName}")
            .parameter(HttpHeaderNames.IF_MATCH.toString(), ParameterIn.HEADER, false, Schema.STRING, "If-Match")
            .parameter(HttpHeaderNames.IF_MODIFIED_SINCE.toString(), ParameterIn.HEADER, false, Schema.STRING, "If-Modified-Since")
            .parameter(RequestHeader.IF_NONE_MATCH, ParameterIn.HEADER, false, Schema.STRING, "If-None-Match")
            .parameter(RequestHeader.IF_UNMODIFIED_SINCE, ParameterIn.HEADER, false, Schema.STRING, "If-Unmodified-Since")
            .create().iterator().next();

      ValidatingRestRequest validating = wrap(Method.GET, "/v3/caches/myCache", invocation);

      assertThatNoException().isThrownBy(validating::getEtagIfMatchHeader);
      assertThatNoException().isThrownBy(validating::getIfModifiedSinceHeader);
      assertThatNoException().isThrownBy(validating::getEtagIfNoneMatchHeader);
      assertThatNoException().isThrownBy(validating::getIfUnmodifiedSinceHeader);
   }

   @Test
   public void testUndeclaredGenericHeaderThrows() {
      Invocation invocation = invocation(Method.GET, "/v3/caches/{cacheName}").create().iterator().next();

      ValidatingRestRequest validating = wrap(Method.GET, "/v3/caches/myCache", invocation);

      assertThatThrownBy(() -> validating.header("X-Custom"))
            .isInstanceOf(UndeclaredParameterException.class)
            .hasMessageContaining("Header")
            .hasMessageContaining("X-Custom");

      assertThatThrownBy(() -> validating.headers("X-Custom"))
            .isInstanceOf(UndeclaredParameterException.class);
   }

   @Test
   public void testDeclaredGenericHeaderAllowed() {
      Invocation invocation = invocation(Method.GET, "/v3/caches/{cacheName}")
            .parameter("X-Custom", ParameterIn.HEADER, false, Schema.STRING, "A custom header")
            .create().iterator().next();

      ValidatingRestRequest validating = wrap(Method.GET, "/v3/caches/myCache", invocation);

      assertThatNoException().isThrownBy(() -> validating.header("X-Custom"));
      assertThatNoException().isThrownBy(() -> validating.headers("X-Custom"));
   }

   @Test
   public void testUndeclaredRequestBodyThrows() {
      Invocation invocation = invocation(Method.PUT, "/v3/caches/{cacheName}").create().iterator().next();

      ValidatingRestRequest validating = wrap(Method.PUT, "/v3/caches/myCache", invocation);

      assertThatThrownBy(validating::contents)
            .isInstanceOf(UndeclaredParameterException.class)
            .hasMessageContaining("Request body");
   }

   @Test
   public void testDeclaredRequestBodyAllowed() {
      Invocation invocation = invocation(Method.PUT, "/v3/caches/{cacheName}")
            .request("Cache configuration", true, Map.of())
            .create().iterator().next();

      RestRequest delegate = new SimpleRequest.Builder()
            .setMethod(Method.PUT)
            .setPath("/v3/caches/myCache")
            .setContents(new ContentSource() {
               @Override
               public String asString() {
                  return "body";
               }

               @Override
               public byte[] rawContent() {
                  return "body".getBytes();
               }

               @Override
               public int size() {
                  return 4;
               }
            })
            .build();

      ValidatingRestRequest validating = new ValidatingRestRequest(delegate, invocation);

      assertThatNoException().isThrownBy(validating::contents);
   }

   @Test
   public void testExemptMethodsPassThrough() {
      Invocation invocation = invocation(Method.GET, "/v3/caches/{cacheName}").create().iterator().next();

      ValidatingRestRequest validating = wrap(Method.GET, "/v3/caches/myCache", invocation);

      assertThatNoException().isThrownBy(validating::method);
      assertThatNoException().isThrownBy(validating::path);
      assertThatNoException().isThrownBy(validating::getAction);
      assertThatNoException().isThrownBy(validating::contentType);
      assertThatNoException().isThrownBy(validating::getAcceptHeader);
      assertThatNoException().isThrownBy(validating::getAuthorizationHeader);
      assertThatNoException().isThrownBy(validating::getCacheControlHeader);
      assertThatNoException().isThrownBy(validating::getContentTypeHeader);
      assertThatNoException().isThrownBy(validating::getRemoteAddress);
      assertThatNoException().isThrownBy(validating::headersKeys);
   }

   private static InvocationImpl.Builder invocation(Method method, String path) {
      return new Invocations.Builder("test", "test invocation")
            .invocation()
            .method(method)
            .path(path)
            .handleWith(r -> null);
   }

   private static RestRequest request(Method method, String path) {
      return new SimpleRequest.Builder()
            .setMethod(method)
            .setPath(path)
            .build();
   }

   private static ValidatingRestRequest wrap(Method method, String path, Invocation invocation) {
      return new ValidatingRestRequest(request(method, path), invocation);
   }
}
