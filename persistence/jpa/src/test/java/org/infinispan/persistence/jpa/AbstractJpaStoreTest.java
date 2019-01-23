package org.infinispan.persistence.jpa;

import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * This is a base class containing various unit tests for each and every different CacheStore implementations. If you
 * need to add Cache/CacheManager tests that need to be run for each cache store/loader implementation, then use
 * BaseCacheStoreFunctionalTest.
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public abstract class AbstractJpaStoreTest extends AbstractInfinispanTest {

   protected static final String PERSISTENCE_UNIT_NAME = "org.infinispan.persistence.jpa";

   protected EmbeddedCacheManager cm;

   protected JpaStore<Object, Object> cs;

   //protected TransactionFactory gtf = new TransactionFactory();

   protected StreamingMarshaller marshaller;

   protected MarshallableEntryFactory entryFactory;

   protected AbstractJpaStoreTest() {
     // gtf.init(false, false, true, false);
   }

   protected EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createCacheManager(true);
   }

   protected JpaStore createCacheStore() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addStore(JpaStoreConfigurationBuilder.class)
            .persistenceUnitName(PERSISTENCE_UNIT_NAME)
            .entityClass(getEntityClass());

      JpaStore store = new JpaStore();
      store.init(new DummyInitializationContext(builder.persistence().stores().get(0).create(), cm.getCache(),
            marshaller, null, entryFactory, new WithinThreadExecutor()));
      store.start();

      assertNotNull(store.getEntityManagerFactory());

      return store;
   }

   protected abstract Class<?> getEntityClass();

   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      try {
         cm = createCacheManager();
         marshaller = cm.getCache().getAdvancedCache().getComponentRegistry().getCacheMarshaller();
         entryFactory = new MarshalledEntryFactoryImpl(marshaller);
         cs = createCacheStore();
         cs.clear();
      } catch (Exception e) {
         log.warn("Error during test setup", e);
         throw e;
      }
   }

   @AfterMethod(alwaysRun = true)
   public void stopMarshaller() {
      //marshaller.stop();
      if (cs != null) cs.stop();
      if (cm != null) cm.stop();
   }

   protected MarshallableEntry createEntry(Object key, Object value) {
      return entryFactory.create(key, value, null);
   }

   protected MarshallableEntry createEntry(TestObject obj) {
      return createEntry(obj.getKey(), obj.getValue());
   }
}
