package org.infinispan.persistence.jpa;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.xsite.irac.persistence.BaseIracPersistenceTest;
import org.testng.annotations.Test;

/**
 * Tests if the IRAC metadata is properly stored and retrieved from a {@link JpaStore}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.jpa.IracJPAStoreTest")
public class IracJPAStoreTest extends BaseIracPersistenceTest<KeyValueEntity> {

   public IracJPAStoreTest() {
      super(JpaKeyValueWrapper.INSTANCE);
   }

   @Override
   protected SerializationContextInitializer getSerializationContextInitializer() {
      return JpaSCI.INSTANCE;
   }

   @Override
   protected void configure(ConfigurationBuilder builder) {
      builder.persistence().addStore(JpaStoreConfigurationBuilder.class)
            .segmented(false)
            .persistenceUnitName("org.infinispan.persistence.jpa")
            .entityClass(KeyValueEntity.class);
   }
}
