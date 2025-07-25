package org.infinispan.client.hotrod.evolution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.client.hotrod.evolution.model.BaseEntityWithNonAnalyzedNameFieldEntity.BaseEntityWithNonAnalyzedNameFieldEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelEntity.BaseModelEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameFieldAnalyzedEntity.BaseModelWithNameFieldAnalyzedEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity.BaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameFieldIndexedEntity.BaseModelWithNameFieldIndexedEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameIndexedAndNameFieldEntity;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameIndexedAndNameFieldEntity.BaseModelWithNameIndexedAndNameFieldEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameIndexedFieldEntity.BaseModelWithNameIndexedFieldEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNewIndexedFieldEntity.BaseModelWithNewIndexedFieldEntitySchema;
import org.infinispan.client.hotrod.evolution.model.ModelUtils;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/*
 * This test class contains use-cases that are necessary in the first phase of Keycloak no-downtime store to work
 *
 * All of these use-cases are build with an assumption that any field that was or will be used in a query is indexed
 * this means
 *   - If we remove index, we stop using the field in queries or remove field
 *   - If we add index we didn't use the field in any query, but now we want to
 */
@Test(groups = "functional", testName = "client.hotrod.evolution.IndexSchemaNoDowntimeUpgradeTest")
public class IndexSchemaNoDowntimeUpgradeTest extends SingleHotRodServerTest {

   private static final String CACHE_NAME = "models";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
            .encoding()
            .mediaType(APPLICATION_PROTOSTREAM_TYPE)
            .indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("evolution.Model");

      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager();
      cacheManager.defineConfiguration(CACHE_NAME, builder.build());

      return cacheManager;
   }

   /*
    * Configure the server, enabling the admin operations
    */
   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());

      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer()
            .host(host)
            .port(serverPort);
      return builder;
   }

   private RemoteCacheManager clientForSchema(GeneratedSchema schema, boolean register) {
      ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();

      // Register proto schema && entity marshaller on client side
      schema.registerSchema(marshaller.getSerializationContext());
      schema.registerMarshallers(marshaller.getSerializationContext());

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = createHotRodClientConfigurationBuilder(hotrodServer.getHost(), hotrodServer.getPort());
      builder.marshaller(marshaller);
      RemoteCacheManager rcm = new RemoteCacheManager(builder.build());

      // Register proto schema on server side
      if (register) {
         rcm.administration().schemas().createOrUpdate(schema);
         // reindexCache would make this test working as well,
         // the difference is that with updateIndexSchema the index state (Lucene directories) is not touched,
         // if the schema change is not retro-compatible reindexCache is required
         rcm.administration().updateIndexSchema(CACHE_NAME);
      }
      return rcm;
   }

   @AfterMethod
   void clean() {
      remoteCacheManager.getCache(CACHE_NAME).clear();
      remoteCacheManager.administration().schemas().remove("evolution-schema.proto", true);
   }

   /*
    * This is used, for example, when a new feature is added
    * the field has no meaning in the older version, so there is no migration needed
    * once the new feature is used, the field is set including the index
    */
   @Test
   void testAddNewFieldWithIndex() {
      // VERSION 1
      try (RemoteCacheManager rcm = clientForSchema(BaseModelEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);

         // Create first version entities
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelEntity(1));

         // Check there is only one with 3 in name field
         doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);
      }

      // VERSION 2
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNewIndexedFieldEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);
         // Create second version entities
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNewIndexedFieldEntity(2));

         // check there are all entities, new and old in the cache
         assertThat(cache.size()).isEqualTo(10);

         // Only new entities contain the new field, no old entity should be returned
         doQuery("FROM evolution.Model WHERE newField LIKE '%3%'", cache, 1);

         // Test lowercase normalizer
         doQuery("FROM evolution.Model WHERE newField LIKE 'Cool%fiEld%3%' ORDER BY newField", cache, 1);
      }
   }

   /*
    * This example shows addition of an index on existing field that was NOT used in any query in the previous version
    *
    * This is used when we add some functionality that needs to search entities by field that existed before, but
    * was not queried
    *
    * Currently, all fields, that are included in some query are indexed
    */
   @Test
   void testAddAIndexOnExistingFieldThatWasNotUsedInAnyQueryBefore() {
      // VERSION 1
      try (RemoteCacheManager rcm = clientForSchema(BaseModelEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);

         // Create VERSION 1 entities
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelEntity(1));

         // This is just for testing, the field is not used in any used query in VERSION 1
         doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);
      }

      // VERSION 2 - note in this version we are NOT able to use the functionality that for the reason for adding index
      // Update to second schema that adds nameIndexed field
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameIndexedAndNameFieldEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);

         // Create VERSION 2 entities
         // Entities created in this version needs to have both name and nameIndexed fields so that VERSION 1 is able to read
         // these entities
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameIndexedAndNameFieldEntity(2));

         // This is just to check data were created correctly, VERSION 2 doesn't use name nor nameIndexed in any query
         // non-indexed field name should be set on both versions new and old
         doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 2);

         // indexed field nameIndexed should be present only in entities created by VERSION 2 as no reindexing was done yet
         doQuery("FROM evolution.Model WHERE analyzed : '*3*'", cache, 1);

         // In this state we can ask administrator to perform update of all entities to VERSION 2 before migrating to VERSION 3
         // The advantage of this approach is:
         //     1. Keycloak is fully functional in the meanwhile
         //     2. Migration can be postponed to any time the administrator wishes (e.g. midnight)
         migrateBaseModelEntityToBaseModelWithNameIndexedAndNameFieldEntity(rcm.getCache(CACHE_NAME));
      }

      // VERSION 3 - note from this version we are able to use the functionality that was the reason for adding the index
      // Update schema to version without name field
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameIndexedFieldEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);
         // Create VERSION 3 entities, entity V3 can't contain name field because BaseModelWithIndexedNameEntity doesn't
         // contain it in other words, migrating all entities to V3 removes name field from all entities
         // The migration is not necessary though the presence of name field should not interfere normal Keycloak behaviour
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameIndexedFieldEntity(3));
      }

      // It is possible there is an older VERSION 2 node writing/reading to/from the cache, it should work
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameIndexedAndNameFieldEntitySchema.INSTANCE, false)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);
         ModelUtils.createModelEntities(cache, 5, i -> ModelUtils.createBaseModelWithNameIndexedAndNameFieldEntity(2).apply(i + 10)); // Creating entities with ids +10 offset
         // This is to check that older version is correctly writing also deprecated name field even though current
         // schema present in the Infinispan server doesn't contain it
         RemoteCache<String, BaseModelWithNameIndexedAndNameFieldEntity> cacheV2Typed = rcm.getCache(CACHE_NAME);
         BaseModelWithNameIndexedAndNameFieldEntity BaseModelWithNameIndexedAndNameFieldEntity = cacheV2Typed.get("300013");
         assertThat(BaseModelWithNameIndexedAndNameFieldEntity.name).isEqualTo("modelD # 13");

         // Query on nameIndexed should work with all entities >= 2 (e.g. all in the storage
         //   because we did the migration `migrateBaseModelEntityToBaseModelWithIndexedAndNonIndexedNameField`)
         doQuery("FROM evolution.Model WHERE analyzed : '*3*'", cache, 4);
      }
   }

   /*
    * This example shows how an index can be removed
    *
    * In this example we assume the reason for removing the index is that it is no longer used in any query anymore
    */
   @Test
   void testRemoveIndexWhenNoLongerUsedInQueryInNewerVersion() {
      // VERSION 1
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameFieldIndexedEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);

         // Create VERSION 1 entities
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldIndexedEntity(1));

         // VERSION 1 uses name in a query
         doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);
      }

      // VERSION 2
      // In this version we cannot remove the index as node of VERSION 1 can still make a query with name in it
      // The important part of this version is to remove all occurrences of the name field in queries

      // VERSION 3
      // In this version we can be sure there is no query with name in it, therefore we can remove the index

      // Update schema to not include index on name
      // Note: If the reason for removal is removal of field completely, the process would be the same with the difference,
      //  that this schema does not contain the field
      try(RemoteCacheManager rcm = clientForSchema(BaseModelEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);
         // Create entities without index
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelEntity(2));

         // Try query with field that has the index in both versions
         doQuery("FROM evolution.Model WHERE entityVersion >= 1", cache, 10);
      }
   }

   @Test
   void testMigrateAnalyzedFieldToNonAnalyzed() {
      // VERSION 1
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameFieldAnalyzedEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);

         // Create VERSION 1 entities with analyzed index
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldAnalyzedEntity(1));
         doQuery("FROM evolution.Model WHERE name : '*3*'", cache, 1);
      }

      // Update schema to VERSION 2 that contains both analyzed and non-analyzed field
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);
         // Create VERSION 2 entities with both indexes
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity(2));

         // We need to do queries that are backward compatible
         doQuery("FROM evolution.Model WHERE (entityVersion < 2 AND name : '*3*') OR (entityVersion >= 2 AND analyzed LIKE '%3%')", cache, 2);

         // In this version we request administrator to migrate to VERSION 2 all entities
         migrateBaseModelWithNameFieldAnalyzedEntitySchemaToBaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity(rcm.getCache(CACHE_NAME));

         // VERSION 3
         // In this version we can stop using query that uses both old and new field because we know that all entities
         //  in the storage are upgraded to VERSION 2
         doQuery("FROM evolution.Model WHERE analyzed LIKE '%3%'", cache, 2);
      }



      // VERSION 4
      // Now we can remove deprecated name field
      try (RemoteCacheManager rcm = clientForSchema(BaseEntityWithNonAnalyzedNameFieldEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseEntityWithNonAnalyzedNameFieldEntity(3));
         doQuery("FROM evolution.Model WHERE analyzed LIKE '%3%'", cache, 3);
      }
   }

   @Test
   void testMigrateNonAnalyzedFieldToAnalyzed() {
      // VERSION 1
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameFieldIndexedEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);

         // Create VERSION 1 entities with non-analyzed index
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldIndexedEntity(1));
         doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);
      }

      // VERSION 2
      // Update schema to VERSION 2 that contains both non-analyzed and analyzed field
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);
         // Create VERSION 2 entities with both indexes
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity(2));

         // We need to do queries that are backward compatible
         doQuery("FROM evolution.Model WHERE (entityVersion < 2 AND name LIKE '%3%') OR (entityVersion >= 2 AND analyzed : '*3*')", cache, 2);
         // In this version we request administrator to migrate to VERSION 2 entities
         migrateBaseModelWithNameFieldIndexedEntityToBaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity(rcm.getCache(CACHE_NAME));

         // VERSION 3
         // In this version we can stop using query that uses both old and new field because we know that all entities
         //  in the storage are upgraded to VERSION 2
         doQuery("FROM evolution.Model WHERE analyzed : '*3*'", cache, 2);
      }

      // VERSION 4
      // Now we can remove deprecated name field
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameIndexedFieldEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameIndexedFieldEntity(3));
         doQuery("FROM evolution.Model WHERE analyzed : '*3*'", cache, 3);
      }
   }

   /**
    * This is the same usecase as {@link IndexSchemaNoDowntimeUpgradeTest#testAddAIndexOnExistingFieldThatWasNotUsedInAnyQueryBefore}
    * <p>
    * The difference is, that it seems for non-analyzed fields it could be possible to do the migration in one step
    * on the other hand, I am not sure whether this works correctly as currently I don't know how to check whether
    * the query used the index or not
    */
   @Test
   void testAddNonAnalyzedIndexOnExistingField() {
      // VERSION 1
      try(RemoteCacheManager rcm = clientForSchema(BaseModelEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);

         // Create VERSION 1 entities
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelEntity(1));

         // Check there is only one with 3 in name field (Query that doesn't use index)
         doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);
      }

      // VERSION 2
      try (RemoteCacheManager rcm = clientForSchema(BaseModelWithNameFieldIndexedEntitySchema.INSTANCE, true)) {
         RemoteCache<String, Model> cache = rcm.getCache(CACHE_NAME);
         // Create second version entities
         ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldIndexedEntity(2));

         // check there are one entry with 3 in name field
         doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);
      }
   }

   private <T> void doQuery(String query, RemoteCache<String, T> messageCache, int expectedResults) {
      Query<T> infinispanObjectEntities = messageCache.query(query);
      List<T> result = StreamSupport.stream(infinispanObjectEntities.spliterator(), false)
            .collect(Collectors.toList());

      assertThat(result).hasSize(expectedResults);
   }

   private Stream doQuery(String query, RemoteCache<String, Model> messageCache) {
      Query<Object[]> infinispanObjectEntities = messageCache.query(query);
      return StreamSupport.stream(infinispanObjectEntities.spliterator(), false);
   }

   private void migrateBaseModelEntityToBaseModelWithNameIndexedAndNameFieldEntity(final RemoteCache<String, BaseModelWithNameIndexedAndNameFieldEntity> cache) {
      new HashSet<>(cache.keySet())
            .forEach(e -> {
               BaseModelWithNameIndexedAndNameFieldEntity BaseModelWithNameIndexedAndNameFieldEntity = cache.get(e);
               if (BaseModelWithNameIndexedAndNameFieldEntity.entityVersion == 1) {
                  BaseModelWithNameIndexedAndNameFieldEntity.analyzed = BaseModelWithNameIndexedAndNameFieldEntity.name;
                  BaseModelWithNameIndexedAndNameFieldEntity.name = null; // TODO: Cannot be null for backward compatibility
                  BaseModelWithNameIndexedAndNameFieldEntity.entityVersion = 2;
                  cache.put(BaseModelWithNameIndexedAndNameFieldEntity.id, BaseModelWithNameIndexedAndNameFieldEntity);
               }
            });
   }

   private void migrateBaseModelWithNameFieldAnalyzedEntitySchemaToBaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity(final RemoteCache<String, BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity> cache) {
      new HashSet<>(cache.keySet())
            .forEach(e -> {
               BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity = cache.get(e);
               if (BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.entityVersion == 1) {
                  BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.analyzed = BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.name;
                  BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.name = null;
                  BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.entityVersion = 2;
                  cache.put(BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.id, BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity);
               }
            });
   }

   private void migrateBaseModelWithNameFieldIndexedEntityToBaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity(final RemoteCache<String, BaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity> cache) {
      new HashSet<>(cache.keySet())
            .forEach(e -> {
               BaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity ModelG = cache.get(e);
               if (ModelG.entityVersion == 1) {
                  ModelG.analyzed = ModelG.name;
                  ModelG.name = null;
                  ModelG.entityVersion = 2;
                  cache.put(ModelG.id, ModelG);
               }
            });
   }
}
