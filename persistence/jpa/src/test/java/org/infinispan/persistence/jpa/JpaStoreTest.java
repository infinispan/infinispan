package org.infinispan.persistence.jpa;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.infinispan.persistence.jpa.impl.EntityManagerFactoryRegistry;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "unit", testName = "persistence.JpaStoreTest")
public class JpaStoreTest extends BaseStoreTest {

   private JpaStore store;
   
   // make this method public to be able to call it from wrapper classes
   @Override
   @AfterMethod
   public void stopMarshaller() {
      super.stopMarshaller();
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      store = new JpaStore();
      JpaStoreConfiguration configuration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
               .persistence()
                  .addStore(JpaStoreConfigurationBuilder.class)
                     .persistenceUnitName("org.infinispan.persistence.jpa")
                     .entityClass(KeyValueEntity.class)
                     .create();
      Cache cache = getCache();
      cache.getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry()
            .registerComponent(new EntityManagerFactoryRegistry(), EntityManagerFactoryRegistry.class);
      store.init(new DummyInitializationContext(configuration, cache, getMarshaller(), new ByteBufferFactoryImpl(),
            new MarshalledEntryFactoryImpl(getMarshaller())));
      store.start();
      return store;
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
