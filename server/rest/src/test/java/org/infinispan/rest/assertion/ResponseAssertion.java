package org.infinispan.rest.assertion;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.MOVED_PERMANENTLY;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_ACCEPTABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PERMANENT_REDIRECT;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.TEMPORARY_REDIRECT;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.infinispan.rest.RequestHeader.CONTENT_ENCODING_HEADER;
import static org.infinispan.rest.ResponseHeader.CACHE_CONTROL_HEADER;
import static org.infinispan.rest.ResponseHeader.CONTENT_LENGTH_HEADER;
import static org.infinispan.rest.ResponseHeader.CONTENT_TYPE_HEADER;
import static org.infinispan.rest.ResponseHeader.DATE_HEADER;
import static org.infinispan.rest.ResponseHeader.ETAG_HEADER;
import static org.infinispan.rest.ResponseHeader.EXPIRES_HEADER;
import static org.infinispan.rest.ResponseHeader.LAST_MODIFIED_HEADER;
import static org.infinispan.rest.ResponseHeader.TRANSFER_ENCODING;
import static org.infinispan.rest.ResponseHeader.WWW_AUTHENTICATE_HEADER;
import static org.testng.AssertJUnit.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.rest.DateUtils;

public class ResponseAssertion {

   private final RestResponse response;

   private ResponseAssertion(RestResponse response) {
      this.response = Objects.requireNonNull(response, "RestResponse cannot be null!");
   }

   public static ResponseAssertion assertThat(CompletionStage<RestResponse> response) {
      CompletableFuture<RestResponse> future = response.toCompletableFuture();
      boolean completed = CompletableFutures.uncheckedAwait(future, 30, TimeUnit.SECONDS);
      if (!completed) {
         Assertions.fail("Timeout obtaining responses");
      }
      return assertThat(future.getNow(null));
   }

   public static ResponseAssertion assertThat(RestResponse response) {
      return new ResponseAssertion(response);
   }

   public ResponseAssertion isOk() {
      if (response.status() >= OK.code() && response.status() <= NO_CONTENT.code()) {
         return this;
      }

      Assertions.fail("Unexpected error code " + response.status() + ": " + response.body());
      return this;
   }

   public ResponseAssertion isRedirect() {
      Assertions.assertThat(response.status()).isIn(MOVED_PERMANENTLY.code(), FOUND.code(), TEMPORARY_REDIRECT.code(), PERMANENT_REDIRECT.code());
      return this;
   }

   public ResponseAssertion doesntExist() {
      Assertions.assertThat(response.status()).isEqualTo(NOT_FOUND.code());
      return this;
   }

   public ResponseAssertion hasReturnedText(String text) {
      Assertions.assertThat(response.body()).isEqualTo(text);
      return this;
   }

   public ResponseAssertion containsInAnyOrderReturnedText(String... text) {
      String body = response.body();
      Assertions.assertThat(body).matches(s -> {
         for (String possible : text) {
            if (s != null && !s.contains(possible)) {
               return false;
            }
         }
         return true;
      }, "Content: " + body + " doesn't contain all of " + Arrays.toString(text));
      return this;
   }

   public ResponseAssertion hasReturnedText(String... textPossibilities) {
      String body = response.body();
      Assertions.assertThat(body).matches(s -> {
         for (String possible : textPossibilities) {
            if (s != null && s.equals(possible)) {
               return true;
            }
         }
         return false;
      }, "Content: " + body + " doesn't match any of " + Arrays.toString(textPossibilities));
      return this;
   }

   public ResponseAssertion containsReturnedText(String text) {
      Assertions.assertThat(response.body()).contains(text);
      return this;
   }

   public ResponseAssertion bodyNotEmpty() {
      Assertions.assertThat(response.body()).isNotEmpty();
      return this;
   }

   public ResponseAssertion hasEtag() {
      Assertions.assertThat(response.headers().get(ETAG_HEADER.getValue())).isNotNull().isNotEmpty();
      return this;
   }

   public ResponseAssertion hasNoContent() {
      Assertions.assertThat(response.body()).isEmpty();
      return this;
   }

   public ResponseAssertion hasNoContentType() {
      Assertions.assertThat(response.headers().get(CONTENT_TYPE_HEADER.getValue())).isNull();
      return this;
   }

   public ResponseAssertion hasNoContentEncoding() {
      Assertions.assertThat(response.headers().get(CONTENT_ENCODING_HEADER.toString())).isNull();
      return this;
   }

   public ResponseAssertion hasContentType(String contentType) {
      Assertions.assertThat(response.header(CONTENT_TYPE_HEADER.getValue()).replace(" ", "")).contains(contentType.replace(" ", ""));
      return this;
   }

   public ResponseAssertion hasContentLength(Integer value) {
      Assertions.assertThat(response.header(CONTENT_LENGTH_HEADER.getValue())).isEqualTo(value.toString());
      return this;
   }

   public ResponseAssertion hasContentLength(Long value) {
      Assertions.assertThat(response.header(CONTENT_LENGTH_HEADER.getValue())).isEqualTo(value.toString());
      return this;
   }

   public ResponseAssertion hasContentEncoding(String encoding) {
      Assertions.assertThat(response.header(CONTENT_ENCODING_HEADER.toString())).isEqualTo(encoding);
      return this;
   }

   public ResponseAssertion hasHeaderMatching(String header, String regexp) {
      Assertions.assertThat(response.header(header)).matches(regexp);
      return this;
   }

   public ResponseAssertion hasHeaderWithValues(String header, String... headers) {
      List<String> expected = Arrays.stream(headers).map(String::toLowerCase).sorted().toList();
      List<String> actual = response.headers().get(header).stream().flatMap(s -> Arrays.stream(s.split(",")))
            .map(String::toLowerCase).sorted().toList();
      assertEquals(expected, actual);
      return this;
   }

   public ResponseAssertion containsAllHeaders(String... headers) {
      String[] expected = Arrays.stream(headers).map(String::toLowerCase).sorted().toArray(String[]::new);
      Assertions.assertThat(response.headers().keySet()).contains(expected);
      return this;
   }

   public ResponseAssertion hasCacheControlHeaders(String... directives) {
      List<String> valueList = response.headers().get(CACHE_CONTROL_HEADER.getValue());
      Assertions.assertThat(valueList).isEqualTo(Arrays.asList(directives));
      return this;
   }

   public ResponseAssertion hasExtendedHeaders() {
      Assertions.assertThat(response.headers().get("Cluster-Primary-Owner")).isNotNull().isNotEmpty();
      Assertions.assertThat(response.headers().get("Cluster-Node-Name")).isNotNull().isNotEmpty();
      Assertions.assertThat(response.headers().get("Cluster-Server-Address")).isNotNull().isNotEmpty();
      return this;
   }

   public ResponseAssertion hasTransferEncoding(String encoding) {
      Assertions.assertThat(response.headers().get(TRANSFER_ENCODING.getValue())).contains(encoding);
      return this;
   }

    public ResponseAssertion hasNotTransferEncoding() {
        Assertions.assertThat(response.headers().get(TRANSFER_ENCODING.getValue())).isNull();
        return this;
    }

   public ResponseAssertion isConflicted() {
      Assertions.assertThat(response.status()).isEqualTo(CONFLICT.code());
      return this;
   }

   public ResponseAssertion isError() {
      Assertions.assertThat(response.status()).isEqualTo(INTERNAL_SERVER_ERROR.code());
      return this;
   }

   public ResponseAssertion isUnauthorized() {
      Assertions.assertThat(response.status()).isEqualTo(UNAUTHORIZED.code());
      Assertions.assertThat(response.headers().get(WWW_AUTHENTICATE_HEADER.getValue())).isNotNull().isNotEmpty();
      return this;
   }

   public ResponseAssertion isForbidden() {
      Assertions.assertThat(response.status()).isEqualTo(FORBIDDEN.code());
      return this;
   }

   public ResponseAssertion isNotFound() {
      Assertions.assertThat(response.status()).isEqualTo(NOT_FOUND.code());
      return this;
   }

   public ResponseAssertion isPayloadTooLarge() {
      Assertions.assertThat(response.status()).isEqualTo(REQUEST_ENTITY_TOO_LARGE.code());
      return this;
   }

   public ResponseAssertion isNotModified() {
      Assertions.assertThat(response.status()).isEqualTo(NOT_MODIFIED.code());
      return this;
   }

   public ResponseAssertion hasContentEqualToFile(String fileName) {
      try {
         Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
         String loadedFile = Files.readString(path);
         Assertions.assertThat(response.body()).isEqualTo(loadedFile);
      } catch (Exception e) {
         throw new AssertionError(e);
      }
      return this;
   }

   public ResponseAssertion isNotAcceptable() {
      Assertions.assertThat(response.status()).isEqualTo(NOT_ACCEPTABLE.code());
      return this;
   }

   public ResponseAssertion isBadRequest() {
      Assertions.assertThat(response.status()).isEqualTo(BAD_REQUEST.code());
      return this;
   }

   public ResponseAssertion hasNoCharset() {
      Assertions.assertThat(response.headers().get(CONTENT_TYPE_HEADER.getValue())).doesNotContain("charset");
      return this;
   }

   public ResponseAssertion hasReturnedBytes(byte[] bytes) {
      Assertions.assertThat(response.bodyAsByteArray()).isEqualTo(bytes);
      return this;
   }

   public ResponseAssertion isServiceUnavailable() {
      Assertions.assertThat(response.status()).isEqualTo(SERVICE_UNAVAILABLE.code());
      return this;
   }

   public ResponseAssertion hasMediaType(MediaType[] mediaType) {
      String contentType = response.header(CONTENT_TYPE_HEADER.getValue());
      boolean hasMatches = Arrays.stream(mediaType).anyMatch(m -> MediaType.fromString(contentType).match(m));
      Assertions.assertThat(hasMatches).isTrue();
      return this;
   }

   public ResponseAssertion hasValidDate() {
      String dateHeader = response.header(DATE_HEADER.getValue());
      ZonedDateTime zonedDateTime = DateUtils.parseRFC1123(dateHeader);
      Assertions.assertThat(zonedDateTime).isNotNull();
      return this;

   }

   public ResponseAssertion hasLastModified(long timestamp) {
      String dateHeader = response.header(LAST_MODIFIED_HEADER.getValue());
      Assertions.assertThat(dateHeader).isNotNull();
      ZonedDateTime zonedDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
      String value = DateTimeFormatter.RFC_1123_DATE_TIME.format(zonedDateTime);
      Assertions.assertThat(value).isEqualTo(dateHeader);
      return this;
   }

   public ResponseAssertion expiresAfter(int expireDuration) {
      String dateHeader = response.header(DATE_HEADER.getValue());
      String expiresHeader = response.header(EXPIRES_HEADER.getValue());

      ZonedDateTime date = DateUtils.parseRFC1123(dateHeader);
      ZonedDateTime expires = DateUtils.parseRFC1123(expiresHeader);

      ZonedDateTime diff = expires.minus(expireDuration, ChronoUnit.SECONDS);
      Assertions.assertThat(diff).isEqualTo(date);
      return this;

   }

   public ResponseAssertion hasNoErrors() {
      hasJson().hasProperty("error").isNull();
      return this;
   }

   public JsonAssertion hasJson() {
      Json node = Json.read(response.body());
      return new JsonAssertion(node);
   }
}
