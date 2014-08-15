package org.infinispan.persistence.jpa;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.infinispan.persistence.jpa.impl.EntityManagerFactoryRegistry;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "unit", testName = "persistence.JpaStoreTest")
public class JpaStoreTest extends BaseStoreTest {

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence()
                  .addStore(JpaStoreConfigurationBuilder.class)
                     .persistenceUnitName(getPersistenceUnitName())
                     .entityClass(KeyValueEntity.class)
                     .storeMetadata(storeMetadata())
                     .create();
      InitializationContext context = createContext(builder.build());
      context.getCache().getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry()
            .registerComponent(new EntityManagerFactoryRegistry(), EntityManagerFactoryRegistry.class);
      JpaStore store = new JpaStore();
      store.init(context);
      return store;
   }

   protected boolean storeMetadata() {
      return true;
   }

   protected String getPersistenceUnitName() {
      return "org.infinispan.persistence.jpa";
   }

   @Override
   public Object wrap(String key, String value) {
      return new KeyValueEntity(key, value);
   }

   @Override
   public String unwrap(Object wrapper) {
      return ((KeyValueEntity) wrapper).getValue();
   }

   @Test(enabled = false)
   @Override
   public void testLoadAndStoreMarshalledValues() throws PersistenceException {
      // disabled as this test cannot be executed on JpaStore
   }
}
