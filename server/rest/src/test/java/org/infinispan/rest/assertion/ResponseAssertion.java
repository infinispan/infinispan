package org.infinispan.rest.assertion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.assertj.core.api.Assertions;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;

public class ResponseAssertion {

   private ContentResponse response;

   private ResponseAssertion(ContentResponse response) {
      this.response = response;
   }

   public static ResponseAssertion assertThat(ContentResponse response) {
      return new ResponseAssertion(response);
   }

   public ResponseAssertion isOk() {
      Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
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
}
