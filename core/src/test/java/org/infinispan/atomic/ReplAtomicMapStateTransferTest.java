package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Test modifications to an AtomicMap during state transfer are consistent across
 * a replicated cluster.
 *
 * @author Ryan Emerson
 * @since 5.2
 */
@Test(groups = "functional", testName = "atomic.ReplAtomicMapStateTransferTest")
public class ReplAtomicMapStateTransferTest extends BaseAtomicMapStateTransferTest {

    public ReplAtomicMapStateTransferTest() {
        super(CacheMode.REPL_SYNC, TransactionMode.TRANSACTIONAL);
    }
}