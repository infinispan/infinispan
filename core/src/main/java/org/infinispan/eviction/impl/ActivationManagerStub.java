package org.infinispan.eviction.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public class ActivationManagerStub implements ActivationManager {
   @Override
   public CompletionStage<Void> activateAsync(Object key, int segment) {
      return CompletableFutures.completedNull();
   }

   @Override
   public long getPendingActivationCount() {
      return 0;
   }

   @Override
   public long getActivationCount() {
      return 0;
   }
}
