package org.infinispan.loaders.jpa;

import org.hibernate.ejb.HibernateEntityManagerFactory;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jpa.entity.User;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test (groups = "functional", testName = "loaders.jdbc.binary.JpaCacheStoreEmfTest")
public class JpaCacheStoreUserEntityTest extends BaseJpaCacheStoreTest {

	@Override
	protected CacheStore createCacheStore() throws Exception {
		JpaCacheStoreConfig config = new JpaCacheStoreConfig();

		config.setPersistenceUnitName("org.infinispan.loaders.jpa");
		config.setEntityClass(User.class);
		config.setPurgeSynchronously(true);

		JpaCacheStore store = new JpaCacheStore();
		store.init(config, cm.getCache(), getMarshaller());
		store.start();

		assert store.getEntityManagerFactory() != null;
		assert store.getEntityManagerFactory() instanceof HibernateEntityManagerFactory;

		return store;
	}

	public void testSimple() throws Exception {
		CacheContainer cm = null;
		try {
			assert cs.getCacheStoreConfig() instanceof JpaCacheStoreConfig;
		} finally {
			TestingUtil.killCacheManagers(cm);
		}
	}

	@Override
	protected TestObject createTestObject(String suffix) {
		User user = new User();
		user.setUsername("u_" + suffix);
		user.setFirstName("fn_" + suffix);
		user.setLastName("ln_" + suffix);
		user.setNote("Some notes " + suffix);

		return new TestObject(user.getUsername(), user);
	}
}
