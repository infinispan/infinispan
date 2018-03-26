/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.collection;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.test.categories.Smoke;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.commons.access.NonTxInvalidationCacheAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.hibernate.cache.commons.access.TxInvalidationCacheAccessDelegate;
import org.hibernate.cache.spi.access.SoftLock;

import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.test.hibernate.cache.commons.AbstractRegionAccessStrategyTest;
import org.infinispan.test.hibernate.cache.commons.NodeEnvironment;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess.TestRegionAccessStrategy;
import org.infinispan.test.hibernate.cache.commons.util.TestSynchronization;
import org.infinispan.test.hibernate.cache.commons.util.TestingKeyFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Base class for tests of CollectionRegionAccessStrategy impls.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
@Category(Smoke.class)
public class CollectionRegionAccessStrategyTest extends
		AbstractRegionAccessStrategyTest<Object> {
	protected static int testCount;

	@Override
	protected Object generateNextKey() {
		return TestingKeyFactory.generateCollectionCacheKey( KEY_BASE + testCount++ );
	}

	@Override
	protected InfinispanBaseRegion getRegion(NodeEnvironment environment) {
		return environment.getCollectionRegion( REGION_NAME, accessType);
	}

	@Override
	protected Object getAccessStrategy(InfinispanBaseRegion region) {
		return TEST_SESSION_ACCESS.collectionAccess(region, accessType);
	}

	@Test
	public void testPutFromLoadRemoveDoesNotProduceStaleData() throws Exception {
		if (!cacheMode.isInvalidation()) {
			return;
		}
		final CountDownLatch pferLatch = new CountDownLatch( 1 );
		final CountDownLatch removeLatch = new CountDownLatch( 1 );
		// remove the interceptor inserted by default PutFromLoadValidator, we're using different one
		PutFromLoadValidator originalValidator = PutFromLoadValidator.removeFromCache(localRegion.getCache());
		PutFromLoadValidator mockValidator = spy(originalValidator);
		doAnswer(invocation -> {
			try {
				return invocation.callRealMethod();
			} finally {
				try {
					removeLatch.countDown();
					// the remove should be blocked because the putFromLoad has been acquired
					// and the remove can continue only after we've inserted the entry
					assertFalse(pferLatch.await( 2, TimeUnit.SECONDS ) );
				}
				catch (InterruptedException e) {
					log.debug( "Interrupted" );
					Thread.currentThread().interrupt();
				}
				catch (Exception e) {
					log.error( "Error", e );
					throw new RuntimeException( "Error", e );
				}
			}
		}).when(mockValidator).acquirePutFromLoadLock(any(), any(), anyLong());
		PutFromLoadValidator.addToCache(localRegion.getCache(), mockValidator);
		cleanup.add(() -> {
			PutFromLoadValidator.removeFromCache(localRegion.getCache());
			PutFromLoadValidator.addToCache(localRegion.getCache(), originalValidator);
		});

		final AccessDelegate delegate = localRegion.getCache().getCacheConfiguration().transaction().transactionMode().isTransactional() ?
			new TxInvalidationCacheAccessDelegate((InfinispanDataRegion) localRegion, mockValidator) :
			new NonTxInvalidationCacheAccessDelegate((InfinispanDataRegion) localRegion, mockValidator);

		ExecutorService executorService = Executors.newCachedThreadPool();
		cleanup.add(() -> executorService.shutdownNow());

		final String KEY = "k1";
		Future<Void> pferFuture = executorService.submit(() -> {
         Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
			delegate.putFromLoad(session, KEY, "v1", SESSION_ACCESS.getTimestamp(session), null);
			return null;
		});

		Future<Void> removeFuture = executorService.submit(() -> {
			removeLatch.await();
         Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
			withTx(localEnvironment, session, () -> {
				delegate.remove(session, KEY);
				return null;
			});
			pferLatch.countDown();
			return null;
		});

		pferFuture.get();
		removeFuture.get();

		assertFalse(localRegion.getCache().containsKey(KEY));
		assertFalse(remoteRegion.getCache().containsKey(KEY));
	}

	@Test
	public void testPutFromLoad() throws Exception {
		putFromLoadTest(false, true);
	}

	@Test
	public void testPutFromLoadMinimal() throws Exception {
		putFromLoadTest(true, true);
	}

	@Override
	protected void doUpdate(TestRegionAccessStrategy strategy, Object session, Object key, TestCacheEntry entry) {
		SoftLock softLock = strategy.lockItem(session, key, null);
		strategy.remove(session, key);
      SESSION_ACCESS.getTransactionCoordinator(session).registerLocalSynchronization(
			new TestSynchronization.UnlockItem(strategy, session, key, softLock));
	}
}
