package org.infinispan.eviction.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.eviction.ActivationManager;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public class ActivationManagerStub implements ActivationManager {
   @Override
   public void onUpdate(Object key, boolean newEntry) {
   }

   @Override
   public void onRemove(Object key, boolean newEntry) {
   }

   @Override
   public CompletableFuture<Void> onUpdateAsync(Object key, boolean newEntry) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> onRemoveAsync(Object key, boolean newEntry) {
      return CompletableFutures.completedNull();
   }

   @Override
   public long getActivationCount() {
      return 0;
   }
}
