package org.infinispan.xsite.events;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * A no-op implementation of {@link XSiteEventsManager}.
 *
 * @since 15.0
 */
public class NoOpXSiteEventsManager implements XSiteEventsManager {

   public static final NoOpXSiteEventsManager INSTANCE = new NoOpXSiteEventsManager();

   private NoOpXSiteEventsManager() {}

   @Override
   public CompletionStage<Void> onLocalEvents(List<XSiteEvent> events) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> onRemoteEvents(List<XSiteEvent> events) {
      return CompletableFutures.completedNull();
   }
}
