/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.entity;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.test.categories.Smoke;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.SoftLock;

import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.test.hibernate.cache.commons.AbstractRegionAccessStrategyTest;
import org.infinispan.test.hibernate.cache.commons.NodeEnvironment;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess.TestRegionAccessStrategy;
import org.infinispan.test.hibernate.cache.commons.util.TestSynchronization;
import org.infinispan.test.hibernate.cache.commons.util.TestingKeyFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Base class for tests of EntityRegionAccessStrategy impls.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
@Category(Smoke.class)
public class EntityRegionAccessStrategyTest extends
		AbstractRegionAccessStrategyTest<Object> {
	protected static int testCount;

	@Override
	protected Object generateNextKey() {
		return TestingKeyFactory.generateEntityCacheKey( KEY_BASE + testCount++ );
	}

	@Override
	protected InfinispanBaseRegion getRegion(NodeEnvironment environment) {
		return environment.getEntityRegion(REGION_NAME, accessType);
	}

	@Override
	protected Object getAccessStrategy(InfinispanBaseRegion region) {
		return TEST_SESSION_ACCESS.entityAccess(region, accessType);
	}

	@Test
	public void testPutFromLoad() throws Exception {
		if (accessType == AccessType.READ_ONLY) {
			putFromLoadTestReadOnly(false);
		} else {
			putFromLoadTest(false, false);
		}
	}

	@Test
	public void testPutFromLoadMinimal() throws Exception {
		if (accessType == AccessType.READ_ONLY) {
			putFromLoadTestReadOnly(true);
		} else {
			putFromLoadTest(true, false);
		}
	}

	@Test
	public void testInsert() throws Exception {
		final Object KEY = generateNextKey();

		final CountDownLatch readLatch = new CountDownLatch(1);
		final CountDownLatch commitLatch = new CountDownLatch(1);
		final CountDownLatch completionLatch = new CountDownLatch(2);

		CountDownLatch asyncInsertLatch = expectAfterUpdate(KEY);

		Thread inserter = new Thread(() -> {
				try {
               Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
					withTx(localEnvironment, session, () -> {
						assertNull("Correct initial value", testLocalAccessStrategy.get(session, KEY, SESSION_ACCESS.getTimestamp(session)));

						doInsert(testLocalAccessStrategy, session, KEY, VALUE1);

						readLatch.countDown();
						commitLatch.await();
						return null;
					});
				} catch (Exception e) {
					log.error("node1 caught exception", e);
					node1Exception = e;
				} catch (AssertionError e) {
					node1Failure = e;
				} finally {

					completionLatch.countDown();
				}
			}, "testInsert-inserter");

		Thread reader = new Thread(() -> {
				try {
					Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
					withTx(localEnvironment, session, () -> {
						readLatch.await();

						assertNull("Correct initial value", testLocalAccessStrategy.get(session, KEY, SESSION_ACCESS.getTimestamp(session)));
						return null;
					});
				} catch (Exception e) {
					log.error("node1 caught exception", e);
					node1Exception = e;
				} catch (AssertionError e) {
					node1Failure = e;
				} finally {
					commitLatch.countDown();
					completionLatch.countDown();
				}
			}, "testInsert-reader");

		inserter.setDaemon(true);
		reader.setDaemon(true);
		inserter.start();
		reader.start();

		assertTrue("Threads completed", completionLatch.await(10, TimeUnit.SECONDS));

		assertThreadsRanCleanly();

		Object s1 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
		assertEquals("Correct node1 value", VALUE1, testLocalAccessStrategy.get(s1, KEY, SESSION_ACCESS.getTimestamp(s1)));

		assertTrue(asyncInsertLatch.await(10, TimeUnit.SECONDS));
		Object expected = isUsingInvalidation() ? null : VALUE1;
		Object s2 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, remoteEnvironment.getRegionFactory());
		assertEquals("Correct node2 value", expected, testRemoteAccessStrategy.get(s2, KEY, SESSION_ACCESS.getTimestamp(s2)));
	}

	protected void doInsert(TestRegionAccessStrategy strategy, Object session, Object key, TestCacheEntry entry) {
		strategy.insert(session, key, entry, entry.getVersion());
      SESSION_ACCESS.getTransactionCoordinator(session).registerLocalSynchronization(
				new TestSynchronization.AfterInsert(strategy, session, key, entry, entry.getVersion()));
	}

	protected void putFromLoadTestReadOnly(boolean minimal) throws Exception {
		final Object KEY = TestingKeyFactory.generateEntityCacheKey( KEY_BASE + testCount++ );

		CountDownLatch remotePutFromLoadLatch = expectPutFromLoad(KEY);

		Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
		withTx(localEnvironment, session, () -> {
			assertNull(testLocalAccessStrategy.get(session, KEY, SESSION_ACCESS.getTimestamp(session)));
			if (minimal)
            testLocalAccessStrategy.putFromLoad(session, KEY, VALUE1, SESSION_ACCESS.getTimestamp(session), VALUE1.getVersion(), true);
			else
            testLocalAccessStrategy.putFromLoad(session, KEY, VALUE1, SESSION_ACCESS.getTimestamp(session), VALUE1.getVersion());
			return null;
		});

		Object s2 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
		assertEquals(VALUE1, testLocalAccessStrategy.get(s2, KEY, SESSION_ACCESS.getTimestamp(s2)));
		Object s3 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, remoteEnvironment.getRegionFactory());
		Object expected;
		if (isUsingInvalidation()) {
			expected = null;
		} else {
			if (accessType != AccessType.NONSTRICT_READ_WRITE) {
				assertTrue(remotePutFromLoadLatch.await(2, TimeUnit.SECONDS));
			}
			expected = VALUE1;
		}
		assertEquals(expected, testRemoteAccessStrategy.get(s3, KEY, SESSION_ACCESS.getTimestamp(s3)));
	}

	@Test
	public void testUpdate() throws Exception {
      log.infof(name.getMethodName());
		if (accessType == AccessType.READ_ONLY) {
			return;
		}

		final Object KEY = generateNextKey();

		// Set up initial state
		Object s1 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
      testLocalAccessStrategy.putFromLoad(s1, KEY, VALUE1, SESSION_ACCESS.getTimestamp(s1), VALUE1.getVersion());
		Object s2 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, remoteEnvironment.getRegionFactory());
      testRemoteAccessStrategy.putFromLoad(s2, KEY, VALUE1, SESSION_ACCESS.getTimestamp(s2), VALUE1.getVersion());

		// both nodes are updated, we don't have to wait for any async replication of putFromLoad
		CountDownLatch asyncUpdateLatch = expectAfterUpdate(KEY);

		final CountDownLatch readLatch = new CountDownLatch(1);
		final CountDownLatch commitLatch = new CountDownLatch(1);
		final CountDownLatch completionLatch = new CountDownLatch(2);

		Thread updater = new Thread(() -> {
				try {
					Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
					withTx(localEnvironment, session, () -> {
						log.debug("Transaction began, get initial value");
						assertEquals("Correct initial value", VALUE1, testLocalAccessStrategy.get(session, KEY, SESSION_ACCESS.getTimestamp(session)));
						log.debug("Now update value");
						doUpdate(testLocalAccessStrategy, session, KEY, VALUE2);
						log.debug("Notify the read latch");
						readLatch.countDown();
						log.debug("Await commit");
						commitLatch.await();
						return null;
					});
				} catch (Exception e) {
					log.error("node1 caught exception", e);
					node1Exception = e;
				} catch (AssertionError e) {
					node1Failure = e;
				} finally {
					if (readLatch.getCount() > 0) {
						readLatch.countDown();
					}
					log.debug("Completion latch countdown");
					completionLatch.countDown();
				}
			}, "testUpdate-updater");

		Thread reader = new Thread(() -> {
				try {
					Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
					withTx(localEnvironment, session, () -> {
						log.debug("Transaction began, await read latch");
						readLatch.await();
						log.debug("Read latch acquired, verify local access strategy");

						// This won't block w/ mvc and will read the old value (if transactional as the transaction
						// is not being committed yet, or if non-strict as we do the actual update only after transaction)
						// or null if non-transactional
						Object expected = isTransactional() || accessType == AccessType.NONSTRICT_READ_WRITE ? VALUE1 : null;
						assertEquals("Correct value", expected, testLocalAccessStrategy.get(session, KEY, SESSION_ACCESS.getTimestamp(session)));
						return null;
					});
				} catch (Exception e) {
					log.error("node1 caught exception", e);
					node1Exception = e;
				} catch (AssertionError e) {
					node1Failure = e;
				} finally {
					commitLatch.countDown();
					log.debug("Completion latch countdown");
					completionLatch.countDown();
				}
			}, "testUpdate-reader");

		updater.setDaemon(true);
		reader.setDaemon(true);
		updater.start();
		reader.start();

		assertTrue(completionLatch.await(2, TimeUnit.SECONDS));

		assertThreadsRanCleanly();

		Object s3 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
		assertEquals("Correct node1 value", VALUE2, testLocalAccessStrategy.get(s3, KEY, SESSION_ACCESS.getTimestamp(s3)));
		assertTrue(asyncUpdateLatch.await(10, TimeUnit.SECONDS));
		Object expected = isUsingInvalidation() ? null : VALUE2;
		Object s4 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, remoteEnvironment.getRegionFactory());
		assertEquals("Correct node2 value", expected, testRemoteAccessStrategy.get(s4, KEY, SESSION_ACCESS.getTimestamp(s4)));
	}

	@Override
	protected void doUpdate(TestRegionAccessStrategy strategy, Object session, Object key, TestCacheEntry entry) {
		SoftLock softLock = strategy.lockItem(session, key, null);
		strategy.update(session, key, entry, null, entry.getVersion());
      SESSION_ACCESS.getTransactionCoordinator(session).registerLocalSynchronization(
				new TestSynchronization.AfterUpdate(strategy, session, key, entry, entry.getVersion(), softLock));
	}

	/**
	 * This test fails in CI too often because it depends on very short timeout. The behaviour is basically
	 * non-testable as we want to make sure that the "Putter" is always progressing; however, it is sometimes
	 * progressing in different thread (on different node), and sometimes even in system, sending a message
	 * over network. Therefore even checking that some OOB/remote thread is in RUNNABLE/RUNNING state is prone
	 * to spurious failure (and we can't grab the state of all threads atomically).
    */
	@Ignore
	@Test
	public void testContestedPutFromLoad() throws Exception {
		if (accessType == AccessType.READ_ONLY) {
			return;
		}

		final Object KEY = TestingKeyFactory.generateEntityCacheKey(KEY_BASE + testCount++);

		Object s1 = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
      testLocalAccessStrategy.putFromLoad(s1, KEY, VALUE1, SESSION_ACCESS.getTimestamp(s1), VALUE1.getVersion());

		final CountDownLatch pferLatch = new CountDownLatch(1);
		final CountDownLatch pferCompletionLatch = new CountDownLatch(1);
		final CountDownLatch commitLatch = new CountDownLatch(1);
		final CountDownLatch completionLatch = new CountDownLatch(1);

		Thread blocker = new Thread("Blocker") {
			@Override
			public void run() {
				try {
					Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
					withTx(localEnvironment, session, () -> {
						assertEquals("Correct initial value", VALUE1, testLocalAccessStrategy.get(session, KEY, SESSION_ACCESS.getTimestamp(session)));

						doUpdate(testLocalAccessStrategy, session, KEY, VALUE2);

						pferLatch.countDown();
						commitLatch.await();
						return null;
					});
				} catch (Exception e) {
					log.error("node1 caught exception", e);
					node1Exception = e;
				} catch (AssertionError e) {
					node1Failure = e;
				} finally {
					completionLatch.countDown();
				}
			}
		};

		Thread putter = new Thread("Putter") {
			@Override
			public void run() {
				try {
					Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
					withTx(localEnvironment, session, () -> {
                  testLocalAccessStrategy.putFromLoad(session, KEY, VALUE1, SESSION_ACCESS.getTimestamp(session), VALUE1.getVersion());
						return null;
					});
				} catch (Exception e) {
					log.error("node1 caught exception", e);
					node1Exception = e;
				} catch (AssertionError e) {
					node1Failure = e;
				} finally {
					pferCompletionLatch.countDown();
				}
			}
		};

		blocker.start();
		assertTrue("Active tx has done an update", pferLatch.await(1, TimeUnit.SECONDS));
		putter.start();
		assertTrue("putFromLoad returns promptly", pferCompletionLatch.await(10, TimeUnit.MILLISECONDS));

		commitLatch.countDown();

		assertTrue("Threads completed", completionLatch.await(1, TimeUnit.SECONDS));

		assertThreadsRanCleanly();

		Object session = TEST_SESSION_ACCESS.mockSession(jtaPlatform, TIME_SERVICE, localEnvironment.getRegionFactory());
		assertEquals("Correct node1 value", VALUE2, testLocalAccessStrategy.get(session, KEY, SESSION_ACCESS.getTimestamp(session)));
	}

   @Test
   @Ignore("ISPN-9175")
   @Override
   public void testRemoveAll() throws Exception {
      super.testRemoveAll();
   }

}
