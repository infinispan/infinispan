package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.InTransactionMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Tests to make sure that when expiration occurs it occurs across the cluster in a scattered cache.
 * This test is needed since scattered cache only works with non transactional caches
 * @author William Burns
 * @since 9.3
 */
@Test(groups = "functional", testName = "expiration.impl.ScatteredExpirationFunctionalTest")
@InCacheMode({CacheMode.SCATTERED_SYNC})
@InTransactionMode({TransactionMode.NON_TRANSACTIONAL})
public class ScatteredExpirationFunctionalTest extends ClusterExpirationFunctionalTest {
}
