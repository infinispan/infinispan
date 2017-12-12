package org.infinispan.test.hibernate.cache.commons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.hibernate.cache.commons.access.SessionAccess;
import org.infinispan.hibernate.cache.commons.impl.BaseRegion;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.commons.util.FutureUpdate;
import org.infinispan.hibernate.cache.commons.util.TombstoneUpdate;
import org.hibernate.cache.internal.CacheDataDescriptionImpl;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.RegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.internal.util.compare.ComparableComparator;

import org.infinispan.hibernate.cache.commons.util.VersionedEntry;
import org.infinispan.test.hibernate.cache.commons.util.ExpectingInterceptor;
import org.infinispan.test.hibernate.cache.commons.util.TestInfinispanRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess.TestRegionAccessStrategy;
import org.infinispan.test.hibernate.cache.commons.util.TestSynchronization;
import org.hibernate.testing.AfterClassOnce;
import org.hibernate.testing.BeforeClassOnce;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.util.ControlledTimeService;
import org.junit.After;
import org.junit.Test;
import junit.framework.AssertionFailedError;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;

import org.jboss.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class AbstractRegionAccessStrategyTest<R extends BaseRegion, S extends RegionAccessStrategy>
		extends AbstractNonFunctionalTest {
	protected final Logger log = Logger.getLogger(getClass());

	public static final String REGION_NAME = "test/com.foo.test";
	public static final String KEY_BASE = "KEY";
	public static final String VALUE1 = "VALUE1";
	public static final String VALUE2 = "VALUE2";
	public static final CacheDataDescription CACHE_DATA_DESCRIPTION
			= new CacheDataDescriptionImpl(true, true, ComparableComparator.INSTANCE, null);

	protected static final ControlledTimeService TIME_SERVICE = new ControlledTimeService();
   protected static final TestSessionAccess TEST_SESSION_ACCESS = TestSessionAccess.findTestSessionAccess();
   protected static final SessionAccess SESSION_ACCESS = SessionAccess.findSessionAccess();

	protected NodeEnvironment localEnvironment;
	protected R localRegion;
	protected S localAccessStrategy;
   protected TestRegionAccessStrategy testLocalAccessStrategy;

	protected NodeEnvironment remoteEnvironment;
	protected R remoteRegion;
   protected S remoteAccessStrategy;
   protected TestRegionAccessStrategy testRemoteAccessStrategy;

	protected boolean transactional;
	protected boolean invalidation;
	protected boolean synchronous;
	protected Exception node1Exception;
	protected Exception node2Exception;
	protected AssertionFailedError node1Failure;
	protected AssertionFailedError node2Failure;

	protected List<Runnable> cleanup = new ArrayList<>();

	@Override
	protected boolean canUseLocalMode() {
		return false;
	}

	@BeforeClassOnce
	public void prepareResources() throws Exception {
		TestResourceTracker.testStarted( getClass().getSimpleName() );
		// to mimic exactly the old code results, both environments here are exactly the same...
		StandardServiceRegistryBuilder ssrb = createStandardServiceRegistryBuilder();
		localEnvironment = new NodeEnvironment( ssrb );
		localEnvironment.prepare();

		localRegion = getRegion(localEnvironment);
		localAccessStrategy = getAccessStrategy(localRegion);
      testLocalAccessStrategy = TEST_SESSION_ACCESS.fromAccessStrategy(localAccessStrategy);

		transactional = Caches.isTransactionalCache(localRegion.getCache());
		invalidation = Caches.isInvalidationCache(localRegion.getCache());
		synchronous = Caches.isSynchronousCache(localRegion.getCache());

		remoteEnvironment = new NodeEnvironment( ssrb );
		remoteEnvironment.prepare();

		remoteRegion = getRegion(remoteEnvironment);
		remoteAccessStrategy = getAccessStrategy(remoteRegion);
      testRemoteAccessStrategy = TEST_SESSION_ACCESS.fromAccessStrategy(remoteAccessStrategy);

		waitForClusterToForm(localRegion.getCache(), remoteRegion.getCache());
	}

	@After
	public void cleanup() {
		cleanup.forEach(Runnable::run);
		cleanup.clear();
		if (localRegion != null) localRegion.getCache().clear();
		if (remoteRegion != null) remoteRegion.getCache().clear();
	}

	@AfterClassOnce
	public void releaseResources() throws Exception {
		try {
			if (localEnvironment != null) {
				localEnvironment.release();
			}
		}
		finally {
			if (remoteEnvironment != null) {
				remoteEnvironment.release();
			}
		}
		TestResourceTracker.testFinished(getClass().getSimpleName());
	}

	@Override
	protected StandardServiceRegistryBuilder createStandardServiceRegistryBuilder() {
		StandardServiceRegistryBuilder ssrb = super.createStandardServiceRegistryBuilder();
		ssrb.applySetting(TestInfinispanRegionFactory.TIME_SERVICE, TIME_SERVICE);
		return ssrb;
	}

	/**
	 * Simulate 2 nodes, both start, tx do a get, experience a cache miss, then
	 * 'read from db.' First does a putFromLoad, then an update (or removal if it is a collection).
	 * Second tries to do a putFromLoad with stale data (i.e. it took longer to read from the db).
	 * Both commit their tx. Then both start a new tx and get. First should see
	 * the updated data; second should either see the updated data
	 * (isInvalidation() == false) or null (isInvalidation() == true).
	 *
	 * @param useMinimalAPI
	 * @param isRemoval
	 * @throws Exception
	 */
	protected void putFromLoadTest(final boolean useMinimalAPI, boolean isRemoval) throws Exception {

		final Object KEY = generateNextKey();

		final CountDownLatch writeLatch1 = new CountDownLatch(1);
		final CountDownLatch writeLatch2 = new CountDownLatch(1);
		final CountDownLatch completionLatch = new CountDownLatch(2);

      CountDownLatch[] putFromLoadLatches = new CountDownLatch[2];

		Thread node1 = new Thread(() -> {
				try {
               Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
               putFromLoadLatches[0] = withTx(localEnvironment, session, () -> {
						assertNull(testLocalAccessStrategy.get(session, KEY, SESSION_ACCESS.getTimestamp(session)));

						writeLatch1.await();

                  CountDownLatch latch = expectPutFromLoad(remoteRegion, KEY);
						if (useMinimalAPI) {
							testLocalAccessStrategy.putFromLoad(session, KEY, VALUE1, SESSION_ACCESS.getTimestamp(session), 1, true);
						} else {
							testLocalAccessStrategy.putFromLoad(session, KEY, VALUE1, SESSION_ACCESS.getTimestamp(session), 1);
						}

						doUpdate(testLocalAccessStrategy, session, KEY, VALUE2, 2);
						return latch;
					});
				} catch (Exception e) {
					log.error("node1 caught exception", e);
					node1Exception = e;
				} catch (AssertionFailedError e) {
					node1Failure = e;
				} finally {
               // Let node2 write
					writeLatch2.countDown();
					completionLatch.countDown();
            }
      }, putFromLoadTestThreadName("node1", useMinimalAPI, isRemoval));

		Thread node2 = new Thread(() -> {
				try {
               Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
               putFromLoadLatches[1] = withTx(remoteEnvironment, session, () -> {

						assertNull(testRemoteAccessStrategy.get(session, KEY, SESSION_ACCESS.getTimestamp(session)));

						// Let node1 write
						writeLatch1.countDown();
						// Wait for node1 to finish
						writeLatch2.await();

                  CountDownLatch latch = expectPutFromLoad(localRegion, KEY);
						if (useMinimalAPI) {
                     testRemoteAccessStrategy.putFromLoad(session, KEY, VALUE1, SESSION_ACCESS.getTimestamp(session), 1, true);
						} else {
                     testRemoteAccessStrategy.putFromLoad(session, KEY, VALUE1, SESSION_ACCESS.getTimestamp(session), 1);
						}
						return latch;
					});
				} catch (Exception e) {
					log.error("node2 caught exception", e);
					node2Exception = e;
				} catch (AssertionFailedError e) {
					node2Failure = e;
				} finally {
					completionLatch.countDown();
				}
      }, putFromLoadTestThreadName("node2", useMinimalAPI, isRemoval));

		node1.setDaemon(true);
		node2.setDaemon(true);

		CountDownLatch remoteUpdate = expectAfterUpdate();

		node1.start();
		node2.start();

		assertTrue("Threads completed", completionLatch.await(2, TimeUnit.SECONDS));

		assertThreadsRanCleanly();
		assertTrue("Update was replicated", remoteUpdate.await(2, TimeUnit.SECONDS));

      // At least one of the put from load latch should have completed
      assertPutFromLoadLatches(putFromLoadLatches);

      Object s1 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertEquals( isRemoval ? null : VALUE2, testLocalAccessStrategy.get(s1, KEY, SESSION_ACCESS.getTimestamp(s1)));
      Object s2 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		Object remoteValue = testRemoteAccessStrategy.get(s2, KEY, SESSION_ACCESS.getTimestamp(s2));
		if (isUsingInvalidation() || isRemoval) {
			// invalidation command invalidates pending put
			assertNull(remoteValue);
		}
		else {
			// The node1 update is replicated, preventing the node2 PFER
			assertEquals( VALUE2, remoteValue);
		}
	}

   protected void assertPutFromLoadLatches(CountDownLatch[] latches) {
      // In some cases, both puts might execute, so make sure you wait for both
      // If waiting directly in the || condition, the right side might not wait
      boolean await0 = await(latches[0]);
      boolean await1 = await(latches[1]);
      assertTrue(String.format(
         "One of the latches in %s should have at least completed", Arrays.toString(latches)),
         await0 || await1);
   }

   private boolean await(CountDownLatch latch) {
      assertNotNull(latch);
      try {
         log.debugf("Await latch: %s", latch);
         boolean await = latch.await(1, TimeUnit.SECONDS);
         log.debugf("Finished waiting for latch, did latch reach zero? %b", await);
         return await;
      } catch (InterruptedException e) {
         // ignore;
         return false;
      }
   }

   String putFromLoadTestThreadName(String node, boolean useMinimalAPI, boolean isRemoval) {
      return String.format("putFromLoad=%s,%s,%s,%s,minimal=%s,isRemove=%s",
         node, mode, cacheMode, accessType, useMinimalAPI, isRemoval);
   }

	protected CountDownLatch expectAfterUpdate() {
		return expectPutWithValue(value -> value instanceof FutureUpdate);
	}

	protected CountDownLatch expectPutWithValue(Predicate<Object> valuePredicate) {
		if (!isUsingInvalidation() && accessType != AccessType.NONSTRICT_READ_WRITE) {
			CountDownLatch latch = new CountDownLatch(1);
			ExpectingInterceptor.get(remoteRegion.getCache())
				.when((ctx, cmd) -> cmd instanceof PutKeyValueCommand && valuePredicate.test(((PutKeyValueCommand) cmd).getValue()))
				.countDown(latch);
			cleanup.add(() -> ExpectingInterceptor.cleanup(remoteRegion.getCache()));
			return latch;
		} else {
			return new CountDownLatch(0);
		}
	}

	protected CountDownLatch expectPutFromLoad() {
		return expectPutWithValue(value -> value instanceof TombstoneUpdate);
	}

   protected CountDownLatch expectPutFromLoad(R region, Object key) {
      Predicate<Object> valuePredicate = accessType == AccessType.NONSTRICT_READ_WRITE
         ? value -> value instanceof VersionedEntry
         : value -> value instanceof TombstoneUpdate;
      CountDownLatch latch;
      latch = new CountDownLatch(1);
      if (!isUsingInvalidation()) {
         latch = new CountDownLatch(1);
         ExpectingInterceptor.get(region.getCache())
            .when((ctx, cmd) -> cmd instanceof PutKeyValueCommand
                  && ((PutKeyValueCommand) cmd).getKey().equals(key)
                  && valuePredicate.test(((PutKeyValueCommand) cmd).getValue()))
            .countDown(latch);
         cleanup.add(() -> ExpectingInterceptor.cleanup(region.getCache()));
      } else {
         if (transactional) {
            expectPutFromLoadEndInvalidating(region, key, latch);
         } else {
            expectInvalidateCommand(region, latch);
         }
      }
      log.debugf("Create latch for putFromLoad: %s", latch);
      return latch;
   }

   protected abstract void doUpdate(TestRegionAccessStrategy strategy, Object session, Object key, Object value, Object version) throws RollbackException, SystemException;

	protected abstract S getAccessStrategy(R region);

	@Test
	public void testRemove() throws Exception {
		evictOrRemoveTest( false );
	}

	@Test
	public void testEvict() throws Exception {
		evictOrRemoveTest( true );
	}

	protected abstract R getRegion(NodeEnvironment environment);

	protected void waitForClusterToForm(Cache... caches) {
		TestingUtil.blockUntilViewsReceived(10000, Arrays.asList(caches));
	}

	protected boolean isTransactional() {
		return transactional;
	}

	protected boolean isUsingInvalidation() {
		return invalidation;
	}

	protected boolean isSynchronous() {
		return synchronous;
	}

	protected void evictOrRemoveTest(final boolean evict) throws Exception {
		final Object KEY = generateNextKey();
		assertEquals(0, localRegion.getCache().size());
		assertEquals(0, remoteRegion.getCache().size());

		CountDownLatch localPutFromLoadLatch = expectRemotePutFromLoad(remoteRegion.getCache(), localRegion.getCache());
		CountDownLatch remotePutFromLoadLatch = expectRemotePutFromLoad(localRegion.getCache(), remoteRegion.getCache());

      Object s1 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertNull("local is clean", testLocalAccessStrategy.get(s1, KEY, SESSION_ACCESS.getTimestamp(s1)));
      Object s2 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertNull("remote is clean", testRemoteAccessStrategy.get(s2, KEY, SESSION_ACCESS.getTimestamp(s2)));

      Object s3 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		testLocalAccessStrategy.putFromLoad(s3, KEY, VALUE1, SESSION_ACCESS.getTimestamp(s3), 1);
      Object s5 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		testRemoteAccessStrategy.putFromLoad(s5, KEY, VALUE1, SESSION_ACCESS.getTimestamp(s5), 1);

		// putFromLoad is applied on local node synchronously, but if there's a concurrent update
		// from the other node it can silently fail when acquiring the loc . Then we could try to read
		// before the update is fully applied.
		assertTrue(localPutFromLoadLatch.await(1, TimeUnit.SECONDS));
		assertTrue(remotePutFromLoadLatch.await(1, TimeUnit.SECONDS));

      Object s4 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertEquals(VALUE1, testLocalAccessStrategy.get(s4, KEY, SESSION_ACCESS.getTimestamp(s4)));
      Object s6 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertEquals(VALUE1, testRemoteAccessStrategy.get(s6, KEY, SESSION_ACCESS.getTimestamp(s6)));

      CountDownLatch endInvalidationLatch = createEndInvalidationLatch(evict, KEY);

      Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		withTx(localEnvironment, session, () -> {
			if (evict) {
				localAccessStrategy.evict(KEY);
			}
			else {
				doRemove(testLocalAccessStrategy, session, KEY);
			}
			return null;
		});

      Object s7 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertNull(testLocalAccessStrategy.get(s7, KEY, SESSION_ACCESS.getTimestamp(s7)));
      Object s8 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertNull(testRemoteAccessStrategy.get(s8, KEY, SESSION_ACCESS.getTimestamp(s8)));

      assertTrue(endInvalidationLatch.await(1, TimeUnit.SECONDS));
      assertEquals(0, localRegion.getCache().size());
		assertEquals(0, remoteRegion.getCache().size());
	}

	protected void doRemove(TestRegionAccessStrategy strategy, Object session, Object key) throws SystemException, RollbackException {
		SoftLock softLock = strategy.lockItem(session, key, null);
		strategy.remove(session, key);
      SessionAccess.TransactionCoordinatorAccess transactionCoordinator = SESSION_ACCESS.getTransactionCoordinator(session);
      transactionCoordinator.registerLocalSynchronization(
				new TestSynchronization.UnlockItem(strategy, session, key, softLock));
	}

	@Test
   public void testRemoveAll() throws Exception {
		evictOrRemoveAllTest(false);
	}

	@Test
   public void testEvictAll() throws Exception {
		evictOrRemoveAllTest(true);
	}

	protected void assertThreadsRanCleanly() {
		if (node1Failure != null) {
			throw node1Failure;
		}
		if (node2Failure != null) {
			throw node2Failure;
		}

		if (node1Exception != null) {
			log.error("node1 saw an exception", node1Exception);
			assertEquals("node1 saw no exceptions", null, node1Exception);
		}

		if (node2Exception != null) {
			log.error("node2 saw an exception", node2Exception);
			assertEquals("node2 saw no exceptions", null, node2Exception);
		}
	}

	protected abstract Object generateNextKey();

	protected void evictOrRemoveAllTest(final boolean evict) throws Exception {
		final Object KEY = generateNextKey();
		assertEquals(0, localRegion.getCache().size());
		assertEquals(0, remoteRegion.getCache().size());
      Object s1 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertNull("local is clean", testLocalAccessStrategy.get(s1, KEY, SESSION_ACCESS.getTimestamp(s1)));
      Object s2 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertNull("remote is clean", testRemoteAccessStrategy.get(s2, KEY, SESSION_ACCESS.getTimestamp(s2)));

		CountDownLatch localPutFromLoadLatch = expectRemotePutFromLoad(remoteRegion.getCache(), localRegion.getCache());
		CountDownLatch remotePutFromLoadLatch = expectRemotePutFromLoad(localRegion.getCache(), remoteRegion.getCache());

      Object s3 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
//      log.infof("Call putFromLoad strategy get for key=%s", KEY);
		testLocalAccessStrategy.putFromLoad(s3, KEY, VALUE1, SESSION_ACCESS.getTimestamp(s3), 1);
      Object s5 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		testRemoteAccessStrategy.putFromLoad(s5, KEY, VALUE1, SESSION_ACCESS.getTimestamp(s5), 1);

		// putFromLoad is applied on local node synchronously, but if there's a concurrent update
		// from the other node it can silently fail when acquiring the loc . Then we could try to read
		// before the update is fully applied.
		assertTrue(localPutFromLoadLatch.await(1, TimeUnit.SECONDS));
		assertTrue(remotePutFromLoadLatch.await(1, TimeUnit.SECONDS));

      Object s4 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
      Object s6 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
//      log.infof("Call local strategy get for key=%s", KEY);
		assertEquals(VALUE1, testLocalAccessStrategy.get(s4, KEY, SESSION_ACCESS.getTimestamp(s4)));
		assertEquals(VALUE1, testRemoteAccessStrategy.get(s6, KEY, SESSION_ACCESS.getTimestamp(s6)));

      CountDownLatch endInvalidationLatch = createEndInvalidationLatch(evict, KEY);

		withTx(localEnvironment, TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE), () -> {
			if (evict) {
				localAccessStrategy.evictAll();
			} else {
				SoftLock softLock = localAccessStrategy.lockRegion();
				localAccessStrategy.removeAll();
				localAccessStrategy.unlockRegion(softLock);
			}
			return null;
		});
      Object s7 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertNull(testLocalAccessStrategy.get(s7, KEY, SESSION_ACCESS.getTimestamp(s7)));
		assertEquals(0, localRegion.getCache().size());

      Object s8 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertNull(testRemoteAccessStrategy.get(s8, KEY, SESSION_ACCESS.getTimestamp(s8)));
		assertEquals(0, remoteRegion.getCache().size());

		// Wait for async propagation of EndInvalidationCommand before executing naked put
		assertTrue(endInvalidationLatch.await(1, TimeUnit.SECONDS));
		TIME_SERVICE.advance(1);

		CountDownLatch lastPutFromLoadLatch = expectRemotePutFromLoad(remoteRegion.getCache(), localRegion.getCache());

		// Test whether the get above messes up the optimistic version
      Object s9 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
      log.infof("Call remote strategy putFromLoad for key=%s and value=%s", KEY, VALUE1);
		assertTrue(testRemoteAccessStrategy.putFromLoad(s9, KEY, VALUE1, SESSION_ACCESS.getTimestamp(s9), 1));
      Object s10 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
      log.infof("Call remote strategy get for key=%s", KEY);
		assertEquals(VALUE1, testRemoteAccessStrategy.get(s10, KEY, SESSION_ACCESS.getTimestamp(s10)));
      // Wait for change to be applied in local, otherwise the count might not be correct
      assertTrue(lastPutFromLoadLatch.await(1, TimeUnit.SECONDS));
		assertEquals(1, remoteRegion.getCache().size());


      Object s11 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertEquals((isUsingInvalidation() ? null : VALUE1), testLocalAccessStrategy.get(s11, KEY, SESSION_ACCESS.getTimestamp(s11)));
      Object s12 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE);
		assertEquals(VALUE1, testRemoteAccessStrategy.get(s12, KEY, SESSION_ACCESS.getTimestamp(s12)));
	}

   private CountDownLatch createEndInvalidationLatch(boolean evict, Object key) {
      CountDownLatch endInvalidationLatch;
      if (invalidation && !evict) {
         // removeAll causes transactional remove commands which trigger EndInvalidationCommands on the remote side
         // if the cache is non-transactional, PutFromLoadValidator.registerRemoteInvalidations cannot find
         // current session nor register tx synchronization, so it falls back to simple InvalidationCommand.
         endInvalidationLatch = new CountDownLatch(1);
         if (transactional) {
            expectPutFromLoadEndInvalidating(remoteRegion, key, endInvalidationLatch);
         } else {
            expectInvalidateCommand(remoteRegion, endInvalidationLatch);
         }
      } else {
         endInvalidationLatch = new CountDownLatch(0);
      }
      log.debugf("Create end invalidation latch: %s", endInvalidationLatch);
      return endInvalidationLatch;
   }

   private void expectPutFromLoadEndInvalidating(R region, Object key, CountDownLatch endInvalidationLatch) {
      PutFromLoadValidator originalValidator = PutFromLoadValidator.removeFromCache(region.getCache());
      assertEquals(PutFromLoadValidator.class, originalValidator.getClass());
      PutFromLoadValidator mockValidator = spy(originalValidator);
      doAnswer(invocation -> {
         try {
            return invocation.callRealMethod();
         } finally {
            log.debugf("Count down latch after calling endInvalidatingKey %s", endInvalidationLatch);
            endInvalidationLatch.countDown();
         }
      }).when(mockValidator).endInvalidatingKey(any(), eq(key));
      PutFromLoadValidator.addToCache(region.getCache(), mockValidator);
      cleanup.add(() -> {
         PutFromLoadValidator.removeFromCache(region.getCache());
         PutFromLoadValidator.addToCache(region.getCache(), originalValidator);
      });
   }

   private void expectInvalidateCommand(R region, CountDownLatch latch) {
      ExpectingInterceptor.get(region.getCache())
         .when((ctx, cmd) -> cmd instanceof InvalidateCommand || cmd instanceof ClearCommand)
         .countDown(latch);
      cleanup.add(() -> ExpectingInterceptor.cleanup(region.getCache()));
   }

   private CountDownLatch expectRemotePutFromLoad(AdvancedCache localCache, AdvancedCache remoteCache) {
		CountDownLatch putFromLoadLatch;
		if (!isUsingInvalidation()) {
			putFromLoadLatch = new CountDownLatch(1);
			// The command may fail to replicate if it can't acquire lock locally
			ExpectingInterceptor.Condition remoteCondition = ExpectingInterceptor.get(remoteCache)
				.when((ctx, cmd) -> !ctx.isOriginLocal() && cmd instanceof PutKeyValueCommand);
			ExpectingInterceptor.Condition localCondition = ExpectingInterceptor.get(localCache)
				.whenFails((ctx, cmd) -> ctx.isOriginLocal() && cmd instanceof PutKeyValueCommand);
			remoteCondition.run(() -> {
				localCondition.cancel();
				putFromLoadLatch.countDown();
			});
			localCondition.run(() -> {
				remoteCondition.cancel();
				putFromLoadLatch.countDown();
			});
			// just for case the test fails and does not remove the interceptor itself
			cleanup.add(() -> ExpectingInterceptor.cleanup(localCache, remoteCache));
		} else {
			putFromLoadLatch = new CountDownLatch(0);
		}
		return putFromLoadLatch;
	}
}
