package org.infinispan.eviction.impl;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@SurvivesRestarts
public class PassivationManagerStub implements PassivationManager {
   @Override
   public boolean isEnabled() {
      return false;
   }

   @Override
   public void passivate(InternalCacheEntry entry) {
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
