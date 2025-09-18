package org.infinispan.client.hotrod.admin;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.client.hotrod.RemoteSchemasAdmin.SchemaOpResultType;
import org.infinispan.client.hotrod.annotation.model.Poem;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.schema.Schema;
import org.infinispan.protostream.schema.Type;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.client.hotrod.RemoteSchemasAdmin.SchemaOpResultType.CREATED;
import static org.infinispan.client.hotrod.RemoteSchemasAdmin.SchemaOpResultType.DELETED;
import static org.infinispan.client.hotrod.RemoteSchemasAdmin.SchemaOpResultType.NONE;
import static org.infinispan.client.hotrod.RemoteSchemasAdmin.SchemaOpResultType.UPDATED;
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

    @Test
    public void testSchemaCrudFromSchema() {
        RemoteSchemasAdmin schemas = remoteCacheManager.administration().schemas();
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
        assertThat(schemas.update(schema).getType()).isEqualTo(NONE);

        // Create  the schema
        assertThat(schemas.create(schema).getType()).isEqualTo(CREATED);

        // Get the schema
        Optional<Schema> existingSchema = schemas.get(schema.getName());
        assertThat(existingSchema).isNotEmpty();
        assertThat(existingSchema.get().getContent()).isEqualTo(schema.getContent());

        // Can't create again, nothing has been done
        assertThat(schemas.create(schema).getType()).isEqualTo(NONE);

        Schema changed = new Schema.Builder(schemaName)
                .packageName("book_sample")
                .addMessage("Author")
                .addField(Type.Scalar.STRING, "name", 1)
                .addField(Type.Scalar.STRING, "surname", 2)
                .addField(Type.Scalar.STRING, "city", 3)
                .build();

        // Update
        assertThat(schemas.update(changed).getType()).isEqualTo(UPDATED);

        // Delete
        assertThat(schemas.remove(schema.getName()).getType()).isEqualTo(DELETED);
        assertThat(schemas.remove(schema.getName()).getType()).isEqualTo(NONE);

        // Get the deleted schema
        assertThat(schemas.get(schema.getName())).isEmpty();
    }

    @Test
    public void testUpdateWithChecks() {
        String schemaName = "updateWithCheck.proto";
        Schema schema = new Schema.Builder(schemaName)
                .addMessage("House")
                .addField(Type.Scalar.STRING, "name", 1)
                .build();
        RemoteSchemasAdmin adminSchemas = remoteCacheManager.administration().schemas();

        assertThat(adminSchemas.create(schema).getType()).isEqualTo(CREATED);
        // We don't change anything
        assertThat(adminSchemas.update(schema).getType()).isEqualTo(NONE);
        assertThat(adminSchemas.update(schema, false).getType()).isEqualTo(NONE);
        // We force replace
        assertThat(adminSchemas.update(schema, true).getType()).isEqualTo(UPDATED);

        Schema schema2 = new Schema.Builder(schemaName)
                .addMessage("Maison")
                .addField(Type.Scalar.STRING, "name", 1)
                .build();
        // We changed the schema
        assertThat(adminSchemas.update(schema2, false).getType()).isEqualTo(UPDATED);
    }

    @Test
    public void testDeleteWithValidationOfUse() {
        Poem.PoemSchema generatedSchema = Poem.PoemSchema.INSTANCE;
        remoteCacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
                .getOrCreateCache("mycache", CONFIG_INDEXED);
        RemoteSchemasAdmin adminSchemas = remoteCacheManager.administration().schemas();
        assertThat(adminSchemas.createOrUpdate(generatedSchema).getType()).isEqualTo(CREATED);

        SchemaOpResultType deleteResult = adminSchemas.remove("nonExistingFile").getType();
        assertThat(deleteResult).isEqualTo(NONE);

        assertThatThrownBy(() -> adminSchemas.remove(generatedSchema.getProtoFileName()))
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("A cache is using one entity in the schema PoemSchema.proto. Remove not done");

        assertThatThrownBy(() -> adminSchemas.remove(generatedSchema.getProtoFileName(), false))
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("A cache is using one entity in the schema PoemSchema.proto. Remove not done");

        deleteResult = adminSchemas.remove(generatedSchema.getProtoFileName(), true).getType();
        assertThat(deleteResult).isEqualTo(DELETED);
    }

    @Test
    public void testCreateOrUpdate() {
        RemoteSchemasAdmin schemasAdministration = remoteCacheManager.administration().schemas();
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

        SchemaOpResultType opResult = schemasAdministration.createOrUpdate(schema).getType();
        assertThat(opResult).isEqualTo(CREATED);

        // No change done
        opResult = schemasAdministration.createOrUpdate(schema).getType();
        assertThat(opResult).isEqualTo(NONE);

        // Do a change
        Schema changedSchema = new Schema.Builder(schema.getName())
                .packageName("book_sample")
                .addMessage("Author")
                .addField(Type.Scalar.STRING, "name", 1)
                .addField(Type.Scalar.STRING, "surname", 2)
                .addField(Type.Scalar.STRING, "city", 3)
                .addField(Type.Scalar.STRING, "country", 4)
                .build();

        opResult = schemasAdministration.createOrUpdate(changedSchema).getType();
        assertThat(opResult).isEqualTo(UPDATED);

        Schema errorSchemaChange = new Schema.Builder(schema.getName())
                .packageName("book_sample")
                .addMessage("Author")
                .addField(Type.Scalar.STRING, "name", 1)
                .addField(Type.Scalar.STRING, "surname", 2)
                .build();

        assertThatThrownBy(() -> schemasAdministration.createOrUpdate(errorSchemaChange))
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("IPROTO000039: Incompatible schema changes");
    }

}
