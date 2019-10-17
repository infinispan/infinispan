package org.infinispan.rest.assertion;

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

import org.assertj.core.api.Assertions;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.DateUtils;

import io.netty.handler.codec.http.HttpHeaderNames;

public class ResponseAssertion {

   private ContentResponse response;

   private ResponseAssertion(ContentResponse response) {
      this.response = response;
   }

   public static ResponseAssertion assertThat(ContentResponse response) {
      return new ResponseAssertion(response);
   }

   public ResponseAssertion isOk() {
      Assertions.assertThat(response.getStatus()).isBetween(HttpStatus.OK_200, HttpStatus.NO_CONTENT_204);
      return this;
   }

   public ResponseAssertion doesntExist() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.NOT_FOUND_404);
      return this;
   }

   public ResponseAssertion hasReturnedText(String text) {
      Assertions.assertThat(response.getContentAsString()).isEqualTo(text);
      return this;
   }

   public ResponseAssertion hasReturnedText(String... textPossibilities) {
      Assertions.assertThat(response.getContentAsString()).matches(s -> {
         for (String possible : textPossibilities) {
            if (s.equals(possible)) {
               return true;
            }
         }
         return false;
      }, "Content: " + response.getContentAsString() + " doesn't match any of " + textPossibilities);
      return this;
   }

   public ResponseAssertion containsReturnedText(String text) {
      Assertions.assertThat(response.getContentAsString()).contains(text);
      return this;
   }

   public ResponseAssertion bodyNotEmpty() {
      Assertions.assertThat(response.getContentAsString()).isNotEmpty();
      return this;
   }

   public ResponseAssertion hasEtag() {
      Assertions.assertThat(response.getHeaders().get(HttpHeader.ETAG)).isNotNull().isNotEmpty();
      return this;
   }

   public ResponseAssertion hasNoContent() {
      Assertions.assertThat(response.getContentAsString()).isEmpty();
      return this;
   }

   public ResponseAssertion hasNoContentType() {
      Assertions.assertThat(response.getHeaders().get(HttpHeader.CONTENT_TYPE)).isNull();
      return this;
   }

   public ResponseAssertion hasNoContentEncoding() {
      Assertions.assertThat(response.getHeaders().get(HttpHeader.CONTENT_ENCODING)).isNull();
      return this;
   }

   public ResponseAssertion hasContentType(String contentType) {
      Assertions.assertThat(response.getHeaders().get(HttpHeader.CONTENT_TYPE).replace(" ", "")).contains(contentType.replace(" ", ""));
      return this;
   }

   public ResponseAssertion hasContentLength(Integer value) {
      Assertions.assertThat(response.getHeaders().get("Content-Length")).isEqualTo(value.toString());
      return this;
   }

   public ResponseAssertion hasContentLength(Long value) {
      Assertions.assertThat(response.getHeaders().get("Content-Length")).isEqualTo(value.toString());
      return this;
   }

   public ResponseAssertion hasGzipContentEncoding() {
      Assertions.assertThat(response.getHeaders().get("Content-Encoding")).isEqualTo("gzip");
      return this;
   }

   public ResponseAssertion hasHeaderMatching(String header, String regexp) {
      Assertions.assertThat(response.getHeaders().get(header)).matches(regexp);
      return this;
   }

   public ResponseAssertion containsAllHeaders(String... headers) {
      Assertions.assertThat(response.getHeaders().stream().map(HttpField::getName)).contains(headers);
      return this;
   }

   public ResponseAssertion hasCacheControlHeaders(String... directives) {
      List<String> valueList = response.getHeaders().getValuesList(HttpHeader.CACHE_CONTROL);
      Assertions.assertThat(valueList).isEqualTo(Arrays.asList(directives));
      return this;
   }

   public ResponseAssertion hasExtendedHeaders() {
      Assertions.assertThat(response.getHeaders().get("Cluster-Primary-Owner")).isNotNull().isNotEmpty();
      Assertions.assertThat(response.getHeaders().get("Cluster-Node-Name")).isNotNull().isNotEmpty();
      Assertions.assertThat(response.getHeaders().get("Cluster-Server-Address")).isNotNull().isNotEmpty();
      return this;
   }

   public ResponseAssertion isConflicted() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.CONFLICT_409);
      return this;
   }

   public ResponseAssertion isError() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR_500);
      return this;
   }

   public ResponseAssertion isUnauthorized() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.UNAUTHORIZED_401);
      Assertions.assertThat(response.getHeaders().get(HttpHeader.WWW_AUTHENTICATE)).isNotNull().isNotEmpty();
      return this;
   }

   public ResponseAssertion isNotFound() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.NOT_FOUND_404);
      return this;
   }

   public ResponseAssertion isPayloadTooLarge() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE_413);
      return this;
   }

   public ResponseAssertion isNotModified() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.NOT_MODIFIED_304);
      return this;
   }

   public ResponseAssertion hasContentEqualToFile(String fileName) {
      try {
         Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
         byte[] loadedFile = Files.readAllBytes(path);
         Assertions.assertThat(response.getContent()).isEqualTo(loadedFile);
      } catch (Exception e) {
         throw new AssertionError(e);
      }
      return this;
   }

   public ResponseAssertion isNotAcceptable() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.NOT_ACCEPTABLE_406);
      return this;
   }

   public ResponseAssertion isBadRequest() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST_400);
      return this;
   }

   public ResponseAssertion hasNoCharset() {
      Assertions.assertThat(response.getHeaders().get(HttpHeader.CONTENT_TYPE)).doesNotContain("charset");
      return this;
   }

   public ResponseAssertion hasReturnedBytes(byte[] bytes) {
      Assertions.assertThat(response.getContent()).containsExactly(bytes);
      return this;
   }

   public ResponseAssertion isServiceUnavailable() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE_503);
      return this;
   }

   public ResponseAssertion hasMediaType(MediaType[] mediaType) {
      boolean hasMatches = Arrays.stream(mediaType).anyMatch(m -> MediaType.fromString(response.getMediaType()).match(m));
      Assertions.assertThat(hasMatches).isTrue();
      return this;
   }

   public ResponseAssertion hasValidDate() {
      String dateHeader = response.getHeaders().get("date");
      ZonedDateTime zonedDateTime = DateUtils.parseRFC1123(dateHeader);
      Assertions.assertThat(zonedDateTime).isNotNull();
      return this;

   }

   public ResponseAssertion hasLastModified(long timestamp) {
      String dateHeader = response.getHeaders().get(HttpHeaderNames.LAST_MODIFIED.toString());
      Assertions.assertThat(dateHeader).isNotNull();
      ZonedDateTime zonedDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
      String value = DateTimeFormatter.RFC_1123_DATE_TIME.format(zonedDateTime);
      Assertions.assertThat(value).isEqualTo(dateHeader);
      return this;
   }

   public ResponseAssertion expiresAfter(int expireDuration) {
      String dateHeader = response.getHeaders().get(HttpHeaderNames.DATE.toString());
      String expiresHeader = response.getHeaders().get(HttpHeaderNames.EXPIRES.toString());

      ZonedDateTime date = DateUtils.parseRFC1123(dateHeader);
      ZonedDateTime expires = DateUtils.parseRFC1123(expiresHeader);

      ZonedDateTime diff = expires.minus(expireDuration, ChronoUnit.SECONDS);
      Assertions.assertThat(diff).isEqualTo(date);
      return this;

   }
}
