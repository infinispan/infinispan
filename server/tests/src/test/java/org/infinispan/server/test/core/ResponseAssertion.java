package org.infinispan.server.test.core;

import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.util.concurrent.CompletableFutures;

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

}
