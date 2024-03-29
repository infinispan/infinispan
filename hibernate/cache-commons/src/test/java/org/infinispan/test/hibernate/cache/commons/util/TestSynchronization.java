package org.infinispan.test.hibernate.cache.commons.util;

import org.hibernate.cache.spi.access.SoftLock;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess.TestRegionAccessStrategy;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class TestSynchronization implements jakarta.transaction.Synchronization {
	protected final Object session;
	protected final Object key;
	protected final Object value;
	protected final Object version;

	public TestSynchronization(Object session, Object key, Object value, Object version) {
		this.session = session;
		this.key = key;
		this.value = value;
		this.version = version;
	}

	@Override
	public void beforeCompletion() {
	}


	public static class AfterInsert extends TestSynchronization {
		private final TestRegionAccessStrategy strategy;

		public AfterInsert(TestRegionAccessStrategy strategy, Object session, Object key, Object value, Object version) {
			super(session, key, value, version);
			this.strategy = strategy;
		}

		@Override
		public void afterCompletion(int status) {
			strategy.afterInsert(session, key, value, version);
		}
	}

	public static class AfterUpdate extends TestSynchronization {
		private final TestRegionAccessStrategy strategy;
		private final SoftLock lock;

		public AfterUpdate(TestRegionAccessStrategy strategy, Object session, Object key, Object value, Object version, SoftLock lock) {
			super(session, key, value, version);
			this.strategy = strategy;
			this.lock = lock;
		}

		@Override
		public void afterCompletion(int status) {
			strategy.afterUpdate(session, key, value, version, null, lock);
		}
	}

	public static class UnlockItem extends TestSynchronization {
		private final TestRegionAccessStrategy strategy;
		private final SoftLock lock;

		public UnlockItem(TestRegionAccessStrategy strategy, Object session, Object key, SoftLock lock) {
			super(session, key, null, null);
			this.strategy = strategy;
			this.lock = lock;
		}

		@Override
		public void afterCompletion(int status) {
			strategy.unlockItem(session, key, lock);
		}
	}
}
