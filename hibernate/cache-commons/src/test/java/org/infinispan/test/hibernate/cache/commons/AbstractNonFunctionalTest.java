/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import javax.transaction.TransactionManager;

import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.engine.transaction.jta.platform.internal.NoJtaPlatform;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.resource.transaction.spi.TransactionStatus;

import org.hibernate.testing.junit4.CustomParameterized;
import org.infinispan.test.hibernate.cache.commons.util.BatchModeJtaPlatform;
import org.infinispan.test.hibernate.cache.commons.util.CacheTestSupport;
import org.infinispan.test.hibernate.cache.commons.util.CacheTestUtil;
import org.infinispan.test.hibernate.cache.commons.util.InfinispanTestingSetup;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.infinispan.configuration.cache.CacheMode;

/**
 * Base class for all non-functional tests of Infinispan integration.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
@RunWith(CustomParameterized.class)
public abstract class AbstractNonFunctionalTest extends org.hibernate.testing.junit4.BaseUnitTestCase {
	@ClassRule
	public static final InfinispanTestingSetup infinispanTestIdentifier = new InfinispanTestingSetup();

   protected static final TestSessionAccess TEST_SESSION_ACCESS = TestSessionAccess.findTestSessionAccess();

	@Parameterized.Parameter(0)
	public String mode;

	@Parameterized.Parameter(1)
	public Class<? extends JtaPlatform> jtaPlatform;

	@Parameterized.Parameter(2)
	public CacheMode cacheMode;

	@Parameterized.Parameter(3)
	public AccessType accessType;

	public static final String REGION_PREFIX = "test";

	private static final String PREFER_IPV4STACK = "java.net.preferIPv4Stack";
	private String preferIPv4Stack;
	private static final String JGROUPS_CFG_FILE = "hibernate.cache.infinispan.jgroups_cfg";
	private String jgroupsCfgFile;

	private CacheTestSupport testSupport = new CacheTestSupport();

	@Parameterized.Parameters(name = "{0}, {2}, {3}")
	public List<Object[]> getParameters() {
		List<Object[]> parameters = new ArrayList<>(Arrays.asList(
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.INVALIDATION_SYNC, AccessType.TRANSACTIONAL},
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.INVALIDATION_SYNC, AccessType.READ_WRITE},
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.INVALIDATION_SYNC, AccessType.READ_ONLY},
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.DIST_SYNC, AccessType.READ_WRITE},
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.DIST_SYNC, AccessType.READ_ONLY},
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.DIST_SYNC, AccessType.NONSTRICT_READ_WRITE},
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.REPL_SYNC, AccessType.READ_WRITE},
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.REPL_SYNC, AccessType.READ_ONLY},
				new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.REPL_SYNC, AccessType.NONSTRICT_READ_WRITE},
				new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.INVALIDATION_SYNC, AccessType.READ_WRITE},
				new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.INVALIDATION_SYNC, AccessType.READ_ONLY},
				new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.DIST_SYNC, AccessType.READ_WRITE},
				new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.DIST_SYNC, AccessType.READ_ONLY},
				new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.DIST_SYNC, AccessType.NONSTRICT_READ_WRITE},
				new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.REPL_SYNC, AccessType.READ_WRITE},
				new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.REPL_SYNC, AccessType.READ_ONLY},
				new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.REPL_SYNC, AccessType.NONSTRICT_READ_WRITE}
		));
		if (canUseLocalMode()) {
			parameters.addAll(Arrays.asList(
					new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.LOCAL, AccessType.TRANSACTIONAL},
					new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.LOCAL, AccessType.READ_WRITE},
					new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.LOCAL, AccessType.READ_ONLY},
					new Object[]{"JTA", BatchModeJtaPlatform.class, CacheMode.LOCAL, AccessType.NONSTRICT_READ_WRITE},
					new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.LOCAL, AccessType.READ_WRITE},
					new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.LOCAL, AccessType.READ_ONLY},
					new Object[]{"non-JTA", NoJtaPlatform.class, CacheMode.LOCAL, AccessType.NONSTRICT_READ_WRITE}
			));
		}
		return parameters;
	}

	@Before
	public void prepareCacheSupport() throws Exception {
		infinispanTestIdentifier.joinContext();
		preferIPv4Stack = System.getProperty(PREFER_IPV4STACK);
		System.setProperty(PREFER_IPV4STACK, "true");
		jgroupsCfgFile = System.getProperty(JGROUPS_CFG_FILE);
		System.setProperty(JGROUPS_CFG_FILE, "2lc-test-tcp.xml");

		testSupport.setUp();
	}

	@After
	public void releaseCachSupport() throws Exception {
		testSupport.tearDown();

		if (preferIPv4Stack == null) {
			System.clearProperty(PREFER_IPV4STACK);
		} else {
			System.setProperty(PREFER_IPV4STACK, preferIPv4Stack);
		}

		if (jgroupsCfgFile == null)
			System.clearProperty(JGROUPS_CFG_FILE);
		else
			System.setProperty(JGROUPS_CFG_FILE, jgroupsCfgFile);
	}

	protected boolean canUseLocalMode() {
		return true;
	}

	protected <T> T withTx(NodeEnvironment environment, Object session, Callable<T> callable) throws Exception {
		TransactionManager tm = environment.getServiceRegistry().getService(JtaPlatform.class).retrieveTransactionManager();
		if (tm != null) {
			return Caches.withinTx(tm, callable);
		} else {
			Transaction transaction = TEST_SESSION_ACCESS.beginTransaction(session);
			boolean rollingBack = false;
			try {
				T retval = callable.call();
				if (transaction.getStatus() == TransactionStatus.ACTIVE) {
					transaction.commit();
				} else {
					rollingBack = true;
					transaction.rollback();
				}
				return retval;
			} catch (Exception e) {
				if (!rollingBack) {
					try {
						transaction.rollback();
					} catch (Exception suppressed) {
						e.addSuppressed(suppressed);
					}
				}
				throw e;
			}
		}
	}

	protected CacheTestSupport getCacheTestSupport() {
		return testSupport;
	}

	protected StandardServiceRegistryBuilder createStandardServiceRegistryBuilder() {
		final StandardServiceRegistryBuilder ssrb = CacheTestUtil.buildBaselineStandardServiceRegistryBuilder(
				REGION_PREFIX, true, false, jtaPlatform);
		ssrb.applySetting(TestRegionFactory.TRANSACTIONAL, TestRegionFactoryProvider.load().supportTransactionalCaches() && accessType == AccessType.TRANSACTIONAL);
		ssrb.applySetting(TestRegionFactory.CACHE_MODE, cacheMode);
		return ssrb;
	}

}
