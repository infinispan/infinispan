package org.infinispan.hibernate.cache.commons;

import java.util.function.Consumer;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;

public enum DataType {
   ENTITY(InfinispanProperties.ENTITY, InfinispanProperties.DEF_ENTITY_RESOURCE, DataType::noValidation),
   NATURAL_ID(InfinispanProperties.NATURAL_ID, InfinispanProperties.DEF_ENTITY_RESOURCE, DataType::noValidation),
   COLLECTION(InfinispanProperties.COLLECTION, InfinispanProperties.DEF_ENTITY_RESOURCE, DataType::noValidation),
   IMMUTABLE_ENTITY(InfinispanProperties.IMMUTABLE_ENTITY, InfinispanProperties.DEF_ENTITY_RESOURCE, DataType::noValidation),
   TIMESTAMPS(InfinispanProperties.TIMESTAMPS, InfinispanProperties.DEF_TIMESTAMPS_RESOURCE, c -> {
      if ( c.clustering().cacheMode().isInvalidation() ) {
         throw log().timestampsMustNotUseInvalidation();
      }
      if (c.memory().isEvictionEnabled()) {
         throw log().timestampsMustNotUseEviction();
      }
   }),
   QUERY(InfinispanProperties.QUERY, InfinispanProperties.DEF_QUERY_RESOURCE, DataType::noValidation),
   PENDING_PUTS(InfinispanProperties.PENDING_PUTS, InfinispanProperties.DEF_PENDING_PUTS_RESOURCE, c -> {
      if (!c.isTemplate()) {
         log().pendingPutsShouldBeTemplate();
      }
      if (c.clustering().cacheMode().isClustered()) {
         throw log().pendingPutsMustNotBeClustered();
      }
      if (c.transaction().transactionMode().isTransactional()) {
         throw log().pendingPutsMustNotBeTransactional();
      }
      if (c.expiration().maxIdle() <= 0) {
         throw log().pendingPutsMustHaveMaxIdle();
      }
   });

   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(InfinispanProperties.class);

   public final String key;
   public final String defaultCacheName;
   private final Consumer<Configuration> validation;

   private static InfinispanMessageLogger log() {
      // we need a method to avoid forward references
      return log;
   }

   private static void noValidation(Configuration c) {
   }

   DataType(String key, String defaultCacheName, Consumer<Configuration> validation) {
      this.key = key;
      this.defaultCacheName = defaultCacheName;
      this.validation = validation;
   }

   public void validate(Configuration configuration) {
      validation.accept(configuration);
   }
}
