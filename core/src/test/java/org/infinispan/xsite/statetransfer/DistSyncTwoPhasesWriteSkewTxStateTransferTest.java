package org.infinispan.xsite.statetransfer;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Tests the cross-site replication with concurrent operations checking for consistency using a distributed synchronous
 * cache with two-phases backup and write skew check with versioning.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.DistSyncTwoPhasesWriteSkewTxStateTransferTest")
public class DistSyncTwoPhasesWriteSkewTxStateTransferTest extends DistSyncTwoPhasesTxStateTransferTest {

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return enableWriteSkew(super.getNycActiveConfig());
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return enableWriteSkew(super.getLonActiveConfig());
   }

   private static ConfigurationBuilder enableWriteSkew(ConfigurationBuilder builder) {
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);
      return builder;
   }
}
