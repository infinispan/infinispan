package org.infinispan.rest.helper;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Exceptions;


/**
 * A utility for managing {@link RestResponse}s in tests.
 *
 * @author Dan Berindei
 * @since 10.1
 */
public class RestResponses {

   public static void assertSuccessful(CompletionStage<RestResponse> responseStage) {
      assertStatus(200, responseStage);
   }

   public static void assertNoContent(CompletionStage<RestResponse> responseStage) {
      assertStatus(204, responseStage);
   }

   public static void assertStatus(int expectedStatus, CompletionStage<RestResponse> responseStage) {
      int status = responseStatus(responseStage);
      assertEquals(expectedStatus, status);
   }

   public static int responseStatus(CompletionStage<RestResponse> responseStage) {
      try (RestResponse response = sync(responseStage)) {
         return response.getStatus();
      }
   }

   public static String responseBody(CompletionStage<RestResponse> responseStage) {
      try (RestResponse response = sync(responseStage)) {
         assertEquals(200, response.getStatus());
         return response.getBody();
      }
   }

   public static Json jsonResponseBody(CompletionStage<RestResponse> responseCompletionStage) {
      return Exceptions.unchecked(() -> Json.read(responseBody(responseCompletionStage)));
   }

   private static <T> T sync(CompletionStage<T> stage) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(10, TimeUnit.SECONDS));
   }

   private static void assertEquals(int expectedStatus, int status) {
      // Create the AssertionError manually so it works in both TestNG and JUnit
      if (status != expectedStatus) {
         throw new AssertionError("Expected: <" + expectedStatus + ">, but was: <" + status + ">");
      }
   }
}
