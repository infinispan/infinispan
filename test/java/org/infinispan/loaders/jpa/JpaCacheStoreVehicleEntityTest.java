package org.infinispan.loaders.jpa;

import org.hibernate.ejb.HibernateEntityManagerFactory;
import org.infinispan.CacheImpl;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jpa.entity.User;
import org.infinispan.loaders.jpa.entity.Vehicle;
import org.infinispan.loaders.jpa.entity.VehicleId;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test (groups = "functional", testName = "loaders.jdbc.binary.JpaCacheStoreEmfTest")
public class JpaCacheStoreVehicleEntityTest extends BaseJpaCacheStoreTest {

	@Override
	protected CacheStore createCacheStore() throws Exception {
		JpaCacheStoreConfig config = new JpaCacheStoreConfig();
		
		config.setPersistenceUnitName("org.infinispan.loaders.jpa");
		config.setEntityClass(Vehicle.class);
		config.setPurgeSynchronously(true);
		
		JpaCacheStore store = new JpaCacheStore();
		store.init(config, cm.getCache(), getMarshaller());
		store.start();
		
		assert store.getEntityManagerFactory() != null;
		assert store.getEntityManagerFactory() instanceof HibernateEntityManagerFactory;
		
		return store;
	}

	@Override
	protected TestObject createTestObject(String key) {
		VehicleId id = new VehicleId("CA" + key, key);
		Vehicle v = new Vehicle();
		v.setId(id);
		v.setColor("c_" + key);
		
		return new TestObject(v.getId(), v);
	}
}
