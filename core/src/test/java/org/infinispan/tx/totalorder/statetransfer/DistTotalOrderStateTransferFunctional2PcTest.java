package org.infinispan.tx.totalorder.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.statetransfer.DistTotalOrderStateTransferFunctional2PcTest")
public class DistTotalOrderStateTransferFunctional2PcTest extends DistTotalOrderStateTransferFunctional1PcTest {

   public DistTotalOrderStateTransferFunctional2PcTest() {
      super("dist-to-2pc-nbst", CacheMode.DIST_SYNC, true, true);
   }

}
