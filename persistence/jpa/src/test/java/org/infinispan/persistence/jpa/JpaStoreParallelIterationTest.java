package org.infinispan.persistence.jpa;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.JpaStoreParallelIterationTest")
public class JpaStoreParallelIterationTest extends ParallelIterationTest {
   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      cb.persistence().addStore(JpaStoreConfigurationBuilder.class)
         .persistenceUnitName(getPersistenceUnitName())
         .entityClass(KeyValueEntity.class)
         .storeMetadata(storeMetadata());
   }

   @Override
   protected int numThreads() {
      return KnownComponentNames.getDefaultThreads(KnownComponentNames.PERSISTENCE_EXECUTOR) + 1;
   }

   @Override
   protected Object wrapKey(int key) {
      return String.valueOf(key);
   }

   @Override
   protected Integer unwrapKey(Object value) {
      return value == null ? null : Integer.parseInt((String) value);
   }

   @Override
   protected Object wrapValue(int key, int value) {
      return new KeyValueEntity(String.valueOf(key), String.valueOf(value));
   }

   @Override
   protected Integer unwrapValue(Object value) {
      return value == null ? null : Integer.parseInt(((KeyValueEntity) value).getValue());
   }

   protected String getPersistenceUnitName() {
      return "org.infinispan.persistence.jpa";
   }

   protected boolean storeMetadata() {
      return true;
   }
}
