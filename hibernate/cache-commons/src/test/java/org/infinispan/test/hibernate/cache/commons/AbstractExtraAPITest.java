package org.infinispan.test.hibernate.cache.commons;

import org.hibernate.cache.spi.access.SoftLock;

import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess.TestRegionAccessStrategy;
import org.infinispan.test.hibernate.cache.commons.util.TestingKeyFactory;
import org.hibernate.testing.AfterClassOnce;
import org.hibernate.testing.BeforeClassOnce;
import org.junit.Test;

import static org.junit.Assert.assertNull;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class AbstractExtraAPITest<S> extends AbstractNonFunctionalTest {

	public static final String REGION_NAME = "test/com.foo.test";
	public static final Object KEY = TestingKeyFactory.generateCollectionCacheKey( "KEY" );
   protected static final TestSessionAccess TEST_SESSION_ACCESS = TestSessionAccess.findTestSessionAccess();
   protected static final Object SESSION = TEST_SESSION_ACCESS.mockSessionImplementor();

	protected S accessStrategy;
   protected TestRegionAccessStrategy testAccessStrategy;
	protected NodeEnvironment environment;

	@BeforeClassOnce
	public final void prepareLocalAccessStrategy() throws Exception {
		environment = new NodeEnvironment( createStandardServiceRegistryBuilder() );
		environment.prepare();

		accessStrategy = getAccessStrategy();
      testAccessStrategy = TEST_SESSION_ACCESS.fromAccess(accessStrategy);
	}

	protected abstract S getAccessStrategy();

	@AfterClassOnce
	public final void releaseLocalAccessStrategy() throws Exception {
		if ( environment != null ) {
			environment.release();
		}
	}

	@Test
	public void testLockItem() {
		assertNull( testAccessStrategy.lockItem(SESSION, KEY, Integer.valueOf( 1 ) ) );
	}

	@Test
	public void testLockRegion() {
		assertNull( testAccessStrategy.lockRegion() );
	}

	@Test
	public void testUnlockItem() {
      testAccessStrategy.unlockItem(SESSION, KEY, new MockSoftLock() );
	}

	@Test
	public void testUnlockRegion() {
      testAccessStrategy.unlockItem(SESSION, KEY, new MockSoftLock() );
	}

	public static class MockSoftLock implements SoftLock {
	}
}
