package org.infinispan.tx.totalorder.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.statetransfer.ReplTotalOrderStateTransferFunctional2PcTest")
public class ReplTotalOrderStateTransferFunctional2PcTest extends ReplTotalOrderStateTransferFunctional1PcTest {

   public ReplTotalOrderStateTransferFunctional2PcTest() {
      super("repl-to-2pc-nbst", CacheMode.REPL_SYNC, true, true, false);
   }

}
