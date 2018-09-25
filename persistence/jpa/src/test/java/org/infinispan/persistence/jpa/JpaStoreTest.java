package org.infinispan.persistence.jpa;

import static org.mockito.Mockito.when;

import java.io.IOException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
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
      GlobalComponentRegistry gcr =
         context.getCache().getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry();
      when(gcr.getComponent(EntityManagerFactoryRegistry.class))
         .thenReturn(new EntityManagerFactoryRegistry());
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

   @Test(enabled = false, description = "Not applicable")
   @Override
   public void testLoadAndStoreBytesValues() throws PersistenceException, IOException, InterruptedException {
      // byte values make no sense for this store
   }
}
