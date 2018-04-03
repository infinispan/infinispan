package org.infinispan.eviction.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.AbstractPassivationManager;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public class PassivationManagerStub extends AbstractPassivationManager {
   @Override
   public boolean isEnabled() {
      return false;
   }

   @Override
   public void passivate(InternalCacheEntry entry) {
   }

   @Override
   public CompletionStage<Void> passivateAsync(InternalCacheEntry entry) {
      return CompletableFutures.completedNull();
   }

   @Override
   public void passivateAll() throws PersistenceException {
   }

   @Override
   public void skipPassivationOnStop(boolean skip) {
      /*no-op*/
   }

   @Override
   public long getPassivations() {
      return 0;
   }

   @Override
   public void resetStatistics() {
   }

   @Override
   public boolean getStatisticsEnabled() {
      return false;
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      throw new UnsupportedOperationException();
   }
}
