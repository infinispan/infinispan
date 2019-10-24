package org.infinispan.persistence.jpa;

import java.lang.reflect.Method;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.protostream.SerializationContextInitializer;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = {"unit", "smoke"}, testName = "persistence.JpaStoreFunctionalTest")
public class JpaStoreFunctionalTest extends BaseStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      persistence.addStore(JpaStoreConfigurationBuilder.class)
            .persistenceUnitName("org.infinispan.persistence.jpa")
            .entityClass(KeyValueEntity.class)
            .preload(preload)
            .segmented(false)
            .create();
      return persistence;
   }

   @Override
   protected SerializationContextInitializer getSerializationContextInitializer() {
      return JpaSCI.INSTANCE;
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
   public void testTwoCachesSameCacheStore() {
      // With JPA, we must use different persistence configurations for each cache
      // as the database name is specified in persistence unit and table name
      // in the entity class.
   }

   @Test(enabled = false, description = "Not applicable")
   public void testPreloadStoredAsBinary() {
      // byte arrays are not entities (no need to test how we can wrap them)
   }

   @Test(enabled = false, description = "Not applicable")
   @Override
   public void testStoreByteArrays(Method m) throws PersistenceException {
      // byte arrays are not entities  (no need to test how we can wrap them)
   }
}
