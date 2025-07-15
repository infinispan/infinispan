package org.infinispan.client.hotrod.admin;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.model.Poem;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.admin.SchemaAdminRuntimeException;
import org.infinispan.commons.admin.SchemasAdministration;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.schema.Schema;
import org.infinispan.protostream.schema.Type;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

@Test(groups = "functional", testName = "client.hotrod.admin.SchemasAdminTest")
public class SchemasAdminTest extends SingleHotRodServerTest {

    public static final String CONFIG_INDEXED = "config-indexed";

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder
                .indexing()
                .enable()
                .storage(LOCAL_HEAP)
                .addIndexedEntity("poem.Poem");

        EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager();
        cacheManager.defineConfiguration(CONFIG_INDEXED, builder.build());

        return cacheManager;
    }

    @Override
    protected RemoteCacheManager getRemoteCacheManager() {
        org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
        builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());

        return new RemoteCacheManager(builder.build());
    }

    @Override
    protected HotRodServer createHotRodServer() {
        HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
        serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());

        return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
    }

    @Test
    public void testSchemaCrudFromSchema() {
        SchemasAdministration schemas = remoteCacheManager.administration().schemas();
        // Get an unexisting schema
        assertThat(schemas.get("nonexisting.proto")).isEmpty();

        String schemaName = "myschema.proto";

        Schema schema = new Schema.Builder(schemaName)
                .packageName("mypackage")
                .addMessage("Person")
                .addField(Type.Scalar.STRING, "name", 1)
                .addField(Type.Scalar.STRING, "surname", 2)
                .build();

        // Update an unexisting schema
        assertThat(schemas.update(schema).isUpdated()).isFalse();

        // Create  the schema
        assertThat(schemas.create(schema).isCreated()).isTrue();

        // Get the schema
        Optional<Schema> existingSchema = schemas.get(schema.getName());
        assertThat(existingSchema).isNotEmpty();
        assertThat(existingSchema.get().getContent()).isEqualTo(schema.getContent());

        // Can't create again
        assertThat(schemas.create(schema).isCreated()).isFalse();
        // Nothing updated if no changed
        assertThat(schemas.update(schema).isUpdated()).isFalse();

        Schema changed = new Schema.Builder(schemaName)
                .packageName("book_sample")
                .addMessage("Author")
                .addField(Type.Scalar.STRING, "name", 1)
                .addField(Type.Scalar.STRING, "surname", 2)
                .addField(Type.Scalar.STRING, "city", 3)
                .build();

        // Update
        assertThat(schemas.update(changed).isUpdated()).isTrue();

        // Delete
        assertThat(schemas.remove(schema.getName()).isDeleted()).isTrue();
        assertThat(schemas.remove(schema.getName()).isDeleted()).isFalse();

        // Get the deleted schema
        assertThat(schemas.get(schema.getName())).isEmpty();
    }

    @Test
    public void testSchemaCrudFromSchemaAsync() {
        SchemasAdministration schemas = remoteCacheManager.administration().schemas();
        String schemaName = "myproto-async.proto";
        Schema schema = new Schema.Builder(schemaName)
                .addMessage("Car")
                .addField(Type.Scalar.STRING, "brand", 1)
                .addField(Type.Scalar.STRING, "year", 2)
                .build();

        // Get an unexisting schema
        assertThat(join(schemas.getAsync(schema.getName()))).isEmpty();

        // Update an unexisting schema
        assertThat(join(schemas.updateAsync(schema)).isUpdated()).isFalse();

        // Create  the schema
        assertThat(join(schemas.createAsync(schema)).isCreated()).isTrue();
        // Get the schema
        Optional<Schema> existingSchema = join(schemas.getAsync(schema.getName()));
        assertThat(existingSchema).isNotEmpty();
        assertThat(existingSchema.get().getContent()).isEqualTo(schema.getContent());

        // Can't create again
        assertThat(join(schemas.createAsync(schema)).isCreated()).isFalse();
        // Nothing updated if no changed
        assertThat(join(schemas.updateAsync(schema)).isUpdated()).isFalse();

        Schema changed = new Schema.Builder(schemaName)
                .packageName("book_sample")
                .addMessage("Author")
                .addField(Type.Scalar.STRING, "name", 1)
                .addField(Type.Scalar.STRING, "surname", 2)
                .addField(Type.Scalar.STRING, "city", 3)
                .build();

        // Update
        assertThat(join(schemas.updateAsync(changed)).isUpdated()).isTrue();

        // Delete
        assertThat(join(schemas.removeAsync(schema.getName())).isDeleted()).isTrue();
        assertThat(join(schemas.removeAsync(schema.getName())).isDeleted()).isFalse();

        // Get the deleted schema
        assertThat(join(schemas.getAsync(schema.getName()))).isEmpty();
    }

    @Test
    public void testUpdateWithChecks() {
        String schemaName = "updateWithCheck.proto";
        Schema schema = new Schema.Builder(schemaName)
                .addMessage("House")
                .addField(Type.Scalar.STRING, "name", 1)
                .build();
        SchemasAdministration adminSchemas = remoteCacheManager.administration().schemas();

        assertThat(adminSchemas.create(schema).isCreated()).isTrue();
        // We don't change anything
        assertThat(adminSchemas.update(schema).isNone()).isTrue();
        assertThat(adminSchemas.update(schema, false).isNone()).isTrue();
        // We force replace
        assertThat(adminSchemas.update(schema, true).isNone()).isTrue();

        Schema schema2 = new Schema.Builder(schemaName)
                .addMessage("Maison")
                .addField(Type.Scalar.STRING, "name", 1)
                .build();
        // We changed the schema
        assertThat(adminSchemas.update(schema2, false).isUpdated()).isTrue();
    }

    @Test
    public void testDeleteWithValidationOfUse() {
        Poem.PoemSchema generatedSchema = Poem.PoemSchema.INSTANCE;
        remoteCacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
                .getOrCreateCache("mycache", CONFIG_INDEXED);
        SchemasAdministration adminSchemas = remoteCacheManager.administration().schemas();
        assertThat(adminSchemas.createOrUpdate(generatedSchema).isCreated()).isTrue();

        SchemasAdministration.SchemaOpResult deleteResult = adminSchemas.remove("nonExistingFile");
        assertThat(deleteResult.isDeleted()).isFalse();

        assertThatThrownBy(() -> adminSchemas.remove(generatedSchema.getProtoFileName()))
                .isInstanceOf(SchemaAdminRuntimeException.class)
                .hasMessageContaining("A cache is using one entity in the schema PoemSchema.proto. Delete not done");

        assertThatThrownBy(() -> adminSchemas.remove(generatedSchema.getProtoFileName(), false))
                .isInstanceOf(SchemaAdminRuntimeException.class)
                .hasMessageContaining("A cache is using one entity in the schema PoemSchema.proto. Delete not done");

        deleteResult = adminSchemas.remove(generatedSchema.getProtoFileName(), true);
        assertThat(deleteResult.isDeleted()).isTrue();
    }

    @Test
    public void testDeleteWithValidationOfUseAsync() {
        Poem.PoemSchema generatedSchema = Poem.PoemSchema.INSTANCE;
        remoteCacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
                .getOrCreateCache("mycache", CONFIG_INDEXED);
        SchemasAdministration adminSchemas = remoteCacheManager.administration().schemas();
        assertThat(join(adminSchemas.createOrUpdateAsync(generatedSchema)).isCreated()).isTrue();

        SchemasAdministration.SchemaOpResult deleteResult = join(adminSchemas.removeAsync("nonExistingFile"));
        assertThat(deleteResult.isDeleted()).isFalse();

        assertThatThrownBy(() -> join(adminSchemas.removeAsync(generatedSchema.getProtoFileName())))
                .isInstanceOf(CompletionException.class)
                .hasCauseExactlyInstanceOf(SchemaAdminRuntimeException.class)
                .hasMessageContaining("A cache is using one entity in the schema PoemSchema.proto. Delete not done");

        assertThatThrownBy(() -> join(adminSchemas.removeAsync(generatedSchema.getProtoFileName(), false)))
                .isInstanceOf(CompletionException.class)
                .hasCauseExactlyInstanceOf(SchemaAdminRuntimeException.class)
                .hasMessageContaining("A cache is using one entity in the schema PoemSchema.proto. Delete not done");

        assertThat(join(adminSchemas.removeAsync(generatedSchema.getProtoFileName(), true)).isDeleted()).isTrue();
        assertThat(join(adminSchemas.getAsync(generatedSchema.getProtoFileName()))).isEmpty();
    }

    @Test
    public void testCreateOrUpdate() {
        SchemasAdministration schemasAdministration = remoteCacheManager.administration().schemas();
        Schema schema = new Schema.Builder("book.proto")
                .packageName("book_sample")
                .addMessage("Author")
                .addField(Type.Scalar.STRING, "name", 1)
                .addField(Type.Scalar.STRING, "surname", 2)
                .addMessage("Book")
                .addField(Type.Scalar.STRING, "title", 1)
                .addField(Type.Scalar.STRING, "description", 2)
                .addField(Type.Scalar.INT32, "publicationYear", 3)
                .addRepeatedField(Type.create("Author"), "author", 4)
                .addField(Type.Scalar.DOUBLE, "price", 5)
                .build();

        SchemasAdministration.SchemaOpResult opResult = schemasAdministration.createOrUpdate(schema);
        assertThat(opResult.isCreated()).isTrue();
        assertThat(opResult.isUpdated()).isFalse();

        // No change done
        opResult = schemasAdministration.createOrUpdate(schema);
        assertThat(opResult.isCreated()).isFalse();
        assertThat(opResult.isUpdated()).isFalse();
        assertThat(opResult.isNone()).isTrue();

        // Do a change
        Schema changedSchema = new Schema.Builder(schema.getName())
                .packageName("book_sample")
                .addMessage("Author")
                .addField(Type.Scalar.STRING, "name", 1)
                .addField(Type.Scalar.STRING, "surname", 2)
                .addField(Type.Scalar.STRING, "city", 3)
                .addField(Type.Scalar.STRING, "country", 4)
                .build();

        opResult = schemasAdministration.createOrUpdate(changedSchema);
        assertThat(opResult.isCreated()).isFalse();
        assertThat(opResult.isUpdated()).isTrue();

        Schema errorSchemaChange = new Schema.Builder(schema.getName())
                .packageName("book_sample")
                .addMessage("Author")
                .addField(Type.Scalar.STRING, "name", 1)
                .addField(Type.Scalar.STRING, "surname", 2)
                .build();

        assertThatThrownBy(() -> schemasAdministration.createOrUpdate(errorSchemaChange))
                .isInstanceOf(HotRodClientException.class)
                .hasMessageContaining("IPROTO000039: Incompatible schema changes");
    }

    @Test
    public void testRetrieveErrorsSyncAndAsync() {
        SchemasAdministration schemas = remoteCacheManager.administration().schemas();
        Schema goodSchema = new Schema.Builder("good.proto")
                .packageName("good")
                .addMessage("Person")
                .addField(Type.Scalar.STRING, "name", 1)
                .build();
        schemas.createOrUpdate(goodSchema);
        schemas.create(Schema.buildFromStringContent("error1.proto", "coucou"));
        schemas.create(Schema.buildFromStringContent("error2.proto", "schema!!"));

        // All schema errors sync
        SchemasAdministration.SchemaErrors schemaErrors = schemas.retrieveAllSchemaErrors();
        assertThat(schemaErrors.getError("good.proto")).isBlank();
        assertThat(schemaErrors.getError("error1.proto")).isEqualTo("Syntax error in error1.proto at 1:6: unexpected label: coucou");
        assertThat(schemaErrors.getError("error2.proto")).isEqualTo("Syntax error in error2.proto at 1:6: unexpected label: schema");

        // All schema errors async
        schemaErrors = join(schemas.retrieveAllSchemaErrorsAsync());
        assertThat(schemaErrors.getError("good.proto")).isBlank();
        assertThat(schemaErrors.getError("error1.proto")).isEqualTo("Syntax error in error1.proto at 1:6: unexpected label: coucou");
        assertThat(schemaErrors.getError("error2.proto")).isEqualTo("Syntax error in error2.proto at 1:6: unexpected label: schema");

        // Individual schema error sync
        assertThat(schemas.retrieveError("good.proto")).isEmpty();
        assertThat(schemas.retrieveError("error2.proto")).isPresent();
        assertThat(schemas.retrieveError("error2.proto").get()).contains("Syntax error in error2.proto");

        // Individual schema error async
        assertThat(join(schemas.retrieveErrorAsync("good.proto"))).isEmpty();
        assertThat(join(schemas.retrieveErrorAsync("error2.proto"))).isPresent();
        assertThat(join(schemas.retrieveErrorAsync("error2.proto")).get()).contains("Syntax error in error2.proto");

    }

}