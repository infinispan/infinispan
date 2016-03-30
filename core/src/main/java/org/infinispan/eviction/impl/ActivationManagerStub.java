package org.infinispan.eviction.impl;

import org.infinispan.eviction.ActivationManager;
import org.infinispan.factories.annotations.SurvivesRestarts;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@SurvivesRestarts
public class ActivationManagerStub implements ActivationManager {
   @Override
   public void onUpdate(Object key, boolean newEntry) {
   }

   @Override
   public void onRemove(Object key, boolean newEntry) {
   }

   @Override
   public long getActivationCount() {
      return 0;
   }
}
