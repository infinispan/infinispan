package org.infinispan.client.hotrod.evolution;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.client.hotrod.evolution.model.BaseEntityWithNonAnalyzedNameFieldEntity.BaseEntityWithNonAnalyzedNameFieldEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelEntity.BaseModelEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameFieldAnalyzedEntity.BaseModelWithNameFieldAnalyzedEntitySchema;
import org.infinispan.client.hotrod.evolution.model.BaseModelWithNameFieldAnalyzedEntityOldAnnotations.BaseModelWithNameFieldAnalyzedEntityOldAnnotationsSchema;
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
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

/**
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

    private final ProtoStreamMarshaller schemaEvolutionClientMarshaller = new ProtoStreamMarshaller();

    /**
     * Configure server side (embedded) cache
     *
     * @return the embedded cache manager
     * @throws Exception
     */
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

    /**
     * Configure the server, enabling the admin operations
     *
     * @return the HotRod server
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
                .port(serverPort)
                .marshaller(schemaEvolutionClientMarshaller);
        return builder;
    }

    private void updateSchemaIndex(GeneratedSchema schema) {
        // Register proto schema && entity marshaller on client side
        schema.registerSchema(schemaEvolutionClientMarshaller.getSerializationContext());
        schema.registerMarshallers(schemaEvolutionClientMarshaller.getSerializationContext());

        // Register proto schema on server side
        RemoteCache<String, String> metadataCache = remoteCacheManager
                .getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
        metadataCache.put(schema.getProtoFileName(), schema.getProtoFile());

        // reindexCache would make this test working as well,
        // the difference is that with updateIndexSchema the index state (Lucene directories) is not touched,
        // if the schema change is not retro-compatible reindexCache is required
        remoteCacheManager.administration().updateIndexSchema(CACHE_NAME);
    }

    /**
     * This is used, for example, when a new feature is added
     * the field has no meaning in the older version, so there is no migration needed
     * once the new feature is used, the field is set including the index
     */
    @Test
    void testAddNewFieldWithIndex() {
        // VERSION 1
        updateSchemaIndex(BaseModelEntitySchema.INSTANCE);
        RemoteCache<String, Model> cache = remoteCacheManager.getCache(CACHE_NAME);

        // Create first version entities
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelEntity(1));

        // Check there is only one with 3 in name field
        doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);

        // VERSION 2
        updateSchemaIndex(BaseModelWithNewIndexedFieldEntitySchema.INSTANCE);

        // Create second version entities
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNewIndexedFieldEntity(2));

        // check there are all entities, new and old in the cache
        assertThat(cache.size()).isEqualTo(10);

        // Only new entities contain the new field, no old entity should be returned
        doQuery("FROM evolution.Model WHERE newField LIKE '%3%'", cache, 1);

        // Test lowercase normalizer
        doQuery("FROM evolution.Model WHERE newField LIKE 'Cool%fiEld%3%' ORDER BY newField", cache, 1);
    }

    /**
     * This example shows addition of an index on existing field that was NOT used in any query in the previous version
     *
     * This is used when we add some functionality that needs to search entities by field that existed before, but
     * was not queried
     *
     * Currently, all fields, that are included in some query are indexed
     *
     */
    @Test
    void testAddAIndexOnExistingFieldThatWasNotUsedInAnyQueryBefore() {
        // VERSION 1
        updateSchemaIndex(BaseModelEntitySchema.INSTANCE);
        RemoteCache<String, Model> cache = remoteCacheManager.getCache(CACHE_NAME);

        // Create VERSION 1 entities
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelEntity(1));

        // This is just for testing, the field is not used in any used query in VERSION 1
        doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);

        // VERSION 2 - note in this version we are NOT able to use the functionality that for the reason for adding index
        // Update to second schema that adds nameIndexed field
        updateSchemaIndex(BaseModelWithNameIndexedAndNameFieldEntitySchema.INSTANCE);

        // Create VERSION 2 entities
        // Entities created in this version needs to have both name and nameIndexed fields so that VERSION 1 is able to read
        // these entities
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameIndexedAndNameFieldEntity(2));

        // This is just to check data were created correctly, VERSION 2 doesn't use name nor nameIndexed in any query
        // non-indexed field name should be set on both versions new and old
        doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 2);

        // indexed field nameIndexed should be present only in entities created by VERSION 2 as no reindexing was done yet
        doQuery("FROM evolution.Model WHERE nameAnalyzed : '*3*'", cache, 1);

        // In this state we can ask administrator to perform update of all entities to VERSION 2 before migrating to VERSION 3
        // The advantage of this approach is:
        //     1. Keycloak is fully functional in the meanwhile
        //     2. Migration can be postponed to any time the administrator wishes (e.g. midnight)
        migrateBaseModelEntityToBaseModelWithNameIndexedAndNameFieldEntity(remoteCacheManager.getCache(CACHE_NAME));

        // VERSION 3 - note from this version we are able to use the functionality that was the reason for adding the index
        // Update schema to version without name field
        updateSchemaIndex(BaseModelWithNameIndexedFieldEntitySchema.INSTANCE);

        // Create VERSION 3 entities, entity V3 can't contain name field because BaseModelWithIndexedNameEntity doesn't
        // contain it in other words, migrating all entities to V3 removes name field from all entities
        // The migration is not necessary though the presence of name field should not interfere normal Keycloak behaviour
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameIndexedFieldEntity(3));

        // It is possible there is an older VERSION 2 node writing/reading to/from the cache, it should work
        BaseModelWithNameIndexedAndNameFieldEntitySchema.INSTANCE.registerSchema(schemaEvolutionClientMarshaller.getSerializationContext());
        BaseModelWithNameIndexedAndNameFieldEntitySchema.INSTANCE.registerMarshallers(schemaEvolutionClientMarshaller.getSerializationContext());
        ModelUtils.createModelEntities(cache, 5, i -> ModelUtils.createBaseModelWithNameIndexedAndNameFieldEntity(2).apply(i + 10)); // Creating entities with ids +10 offset

        // This is to check that older version is correctly writing also deprecated name field even though current
        // schema present in the Infinispan server doesn't contain it
        RemoteCache<String, BaseModelWithNameIndexedAndNameFieldEntity> cacheV2Typed = remoteCacheManager.getCache(CACHE_NAME);
        BaseModelWithNameIndexedAndNameFieldEntity BaseModelWithNameIndexedAndNameFieldEntity = cacheV2Typed.get("300013");
        assertThat(BaseModelWithNameIndexedAndNameFieldEntity.name).isEqualTo("modelD # 13");

        // Query on nameIndexed should work with all entities >= 2 (e.g. all in the storage
        //   because we did the migration `migrateBaseModelEntityToBaseModelWithIndexedAndNonIndexedNameField`)
        doQuery("FROM evolution.Model WHERE nameAnalyzed : '*3*'", cache, 4);
    }

    /**
     * This example shows how an index can be removed
     *
     * In this example we assume the reason for removing the index is that it is no longer used in any query anymore
     */
    @Test
    void testRemoveIndexWhenNoLongerUsedInQueryInNewerVersion() {
        // VERSION 1
        updateSchemaIndex(BaseModelWithNameFieldIndexedEntitySchema.INSTANCE);
        RemoteCache<String, Model> cache = remoteCacheManager.getCache(CACHE_NAME);

        // Create VERSION 1 entities
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldIndexedEntity(1));

        // VERSION 1 uses name in a query
        doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);

        // VERSION 2
        // In this version we cannot remove the index as node of VERSION 1 can still make a query with name in it
        // The important part of this version is to remove all occurrences of the name field in queries

        // VERSION 3
        // In this version we can be sure there is no query with name in it, therefore we can remove the index

        // Update schema to not include index on name
        // Note: If the reason for removal is removal of field completely, the process would be the same with the difference,
        //  that this schema does not contain the field
        updateSchemaIndex(BaseModelEntitySchema.INSTANCE);

        // Create entities without index
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelEntity(2));

        // Try query with field that has the index in both versions
        doQuery("FROM evolution.Model WHERE entityVersion >= 1", cache, 10);
    }

    @Test
    void testMigrateAnalyzedFieldToNonAnalyzed() {
        // VERSION 1
        updateSchemaIndex(BaseModelWithNameFieldAnalyzedEntitySchema.INSTANCE);
        RemoteCache<String, Model> cache = remoteCacheManager.getCache(CACHE_NAME);

        // Create VERSION 1 entities with analyzed index
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldAnalyzedEntity(1));
        doQuery("FROM evolution.Model WHERE nameAnalyzed : '*3*'", cache, 1);

        // Update schema to VERSION 2 that contains both analyzed and non-analyzed field
        updateSchemaIndex(BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntitySchema.INSTANCE);

        // Create VERSION 2 entities with both indexes
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity(2));

        // We need to do queries that are backward compatible
        doQuery("FROM evolution.Model WHERE (entityVersion < 2 AND nameAnalyzed : '*3*') OR (entityVersion >= 2 AND nameNonAnalyzed LIKE '%3%')", cache, 2);

        // In this version we request administrator to migrate to VERSION 2 all entities
        migrateBaseModelWithNameFieldAnalyzedEntitySchemaToBaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity(remoteCacheManager.getCache(CACHE_NAME));

        // VERSION 3
        // In this version we can stop using query that uses both old and new field because we know that all entities
        //  in the storage are upgraded to VERSION 2
        doQuery("FROM evolution.Model WHERE nameNonAnalyzed LIKE '%3%'", cache, 2);

        // VERSION 4
        // Now we can remove deprecated name field
        updateSchemaIndex(BaseEntityWithNonAnalyzedNameFieldEntitySchema.INSTANCE);

        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseEntityWithNonAnalyzedNameFieldEntity(3));
        doQuery("FROM evolution.Model WHERE nameNonAnalyzed LIKE '%3%'", cache, 3);
    }

    @Test
    void testMigrateNonAnalyzedFieldToAnalyzed() {
        // VERSION 1
        updateSchemaIndex(BaseModelWithNameFieldIndexedEntitySchema.INSTANCE);
        RemoteCache<String, Model> cache = remoteCacheManager.getCache(CACHE_NAME);

        // Create VERSION 1 entities with non-analyzed index
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldIndexedEntity(1));
        doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);

        // VERSION 2
        // Update schema to VERSION 2 that contains both non-analyzed and analyzed field
        updateSchemaIndex(BaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntitySchema.INSTANCE);

        // Create VERSION 2 entities with both indexes
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity(2));

        // We need to do queries that are backward compatible
        doQuery("FROM evolution.Model WHERE (entityVersion < 2 AND name LIKE '%3%') OR (entityVersion >= 2 AND nameAnalyzed : '*3*')", cache, 2);

        // In this version we request administrator to migrate to VERSION 2 entities
        migrateBaseModelWithNameFieldIndexedEntityToBaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity(remoteCacheManager.getCache(CACHE_NAME));

        // VERSION 3
        // In this version we can stop using query that uses both old and new field because we know that all entities
        //  in the storage are upgraded to VERSION 2
        doQuery("FROM evolution.Model WHERE nameAnalyzed : '*3*'", cache, 2);

        // VERSION 4
        // Now we can remove deprecated name field
        updateSchemaIndex(BaseModelWithNameIndexedFieldEntitySchema.INSTANCE);

        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameIndexedFieldEntity(3));
        doQuery("FROM evolution.Model WHERE nameAnalyzed : '*3*'", cache, 3);
    }

    @Test
    void testMigrateOldAnnotationToNewOne() {
        // VERSION 1
        updateSchemaIndex(BaseModelWithNameFieldAnalyzedEntityOldAnnotationsSchema.INSTANCE);
        RemoteCache<String, Model> cache = remoteCacheManager.getCache(CACHE_NAME);

        // Create VERSION 1 entities
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldAnalyzedEntityOldAnnotations(1));

        // VERSION 1 uses name in a query
        doQuery("FROM evolution.Model WHERE nameAnalyzed : '*3*'", cache, 1);

        // VERSION 2
        updateSchemaIndex(BaseModelWithNameFieldAnalyzedEntitySchema.INSTANCE);

        // No update/reindexing needed as ModelB and ModelJ should be equivalent

        // Create entities without index
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldAnalyzedEntity(2));

        // Try query with field that has the index in both versions
        doQuery("FROM evolution.Model WHERE nameAnalyzed : '*3*'", cache, 2);
    }

    /**
     * This is the same usecase as {@link IndexSchemaNoDowntimeUpgradeTest#testAddAIndexOnExistingFieldThatWasNotUsedInAnyQueryBefore}
     *
     * The difference is, that it seems for non-analyzed fields it could be possible to do the migration in one step
     * on the other hand, I am not sure whether this works correctly as currently I don't know how to check whether
     * the query used the index or not
     */
    @Test
    void testAddNonAnalyzedIndexOnExistingField() {
        // VERSION 1
        updateSchemaIndex(BaseModelEntitySchema.INSTANCE);
        RemoteCache<String, Model> cache = remoteCacheManager.getCache(CACHE_NAME);

        // Create VERSION 1 entities
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelEntity(1));

        // Check there is only one with 3 in name field (Query that doesn't use index)
        doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 1);

        // VERSION 2
        updateSchemaIndex(BaseModelWithNameFieldIndexedEntitySchema.INSTANCE);

        // Create second version entities
        ModelUtils.createModelEntities(cache, 5, ModelUtils.createBaseModelWithNameFieldIndexedEntity(2));

        // check there are two entities with 3 in name field
        doQuery("FROM evolution.Model WHERE name LIKE '%3%'", cache, 2);
    }

    private <T> void doQuery(String query, RemoteCache<String, T> messageCache, int expectedResults) {
        QueryFactory queryFactory = Search.getQueryFactory(messageCache);
        Query<T> infinispanObjectEntities = queryFactory.create(query);
        Set<T> result = StreamSupport.stream(infinispanObjectEntities.spliterator(), false)
                .collect(Collectors.toSet());

        assertThat(result).hasSize(expectedResults);
    }

    private void migrateBaseModelEntityToBaseModelWithNameIndexedAndNameFieldEntity(final RemoteCache<String, BaseModelWithNameIndexedAndNameFieldEntity> cache) {
        new HashSet<>(cache.keySet())
                .forEach(e -> {
                    BaseModelWithNameIndexedAndNameFieldEntity BaseModelWithNameIndexedAndNameFieldEntity = cache.get(e);
                    if (BaseModelWithNameIndexedAndNameFieldEntity.entityVersion == 1) {
                        BaseModelWithNameIndexedAndNameFieldEntity.nameAnalyzed = BaseModelWithNameIndexedAndNameFieldEntity.name;
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
                        BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.nameNonAnalyzed = BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.nameAnalyzed;
                        BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity.nameAnalyzed = null;
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
                        ModelG.nameAnalyzed = ModelG.name;
                        ModelG.name = null;
                        ModelG.entityVersion = 2;
                        cache.put(ModelG.id, ModelG);
                    }
                });
    }
}
