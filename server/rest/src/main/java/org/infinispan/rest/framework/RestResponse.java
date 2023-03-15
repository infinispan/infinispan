package org.infinispan.rest.framework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * @since 10.0
 */
public interface RestResponse {

   int getStatus();

   Object getEntity();

   default CompletionStage<RestResponse> toFuture() {
      return CompletableFuture.completedFuture(this);
   }
}
