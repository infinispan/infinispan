package org.infinispan.lock.impl;

import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ClusteredLockBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // The creation of an initial lock is blocking
      // https://issues.redhat.com/browse/ISPN-11835
      builder.allowBlockingCallsInside(EmbeddedClusteredLockManager.class.getName(), "createLock");
      builder.allowBlockingCallsInside(EmbeddedClusteredLockManager.class.getName(), "defineLock");
   }
}
