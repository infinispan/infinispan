package org.infinispan.manager;

import org.infinispan.commons.admin.SchemasAdministration;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.schema.Schema;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class EmbeddedSchemasAdmin implements SchemasAdministration {
    @Override
    public Optional<Schema> get(String schemaName) {
        return Optional.empty();
    }

    @Override
    public CompletionStage<Optional<Schema>> getAsync(String schemaName) {
        return null;
    }

    @Override
    public SchemaOpResult create(Schema schema) {
        return null;
    }

    @Override
    public Map<String, SchemaOpResult> create(FileDescriptorSource fileDescriptorSource) {
        return null;
    }

    @Override
    public CompletionStage<SchemaOpResult> createAsync(Schema schema) {
        return null;
    }

    @Override
    public SchemaOpResult update(Schema schema) {
        return null;
    }

    @Override
    public SchemaOpResult update(Schema schema, boolean force) {
        return null;
    }

    @Override
    public CompletionStage<SchemaOpResult> updateAsync(Schema schema, boolean force) {
        return null;
    }

    private SchemaOpResult update(String name, String schemaContent) {
        return null;
    }

    @Override
    public CompletionStage<SchemaOpResult> updateAsync(Schema schema) {
        return null;
    }

    @Override
    public Map<String, SchemaOpResult> createOrUpdate(FileDescriptorSource fileDescriptorSource) {
        return Map.of();
    }

    @Override
    public SchemaOpResult createOrUpdate(Schema schemas) {
        return null;
    }

    @Override
    public SchemaOpResult createOrUpdate(Schema schema, boolean force) {
        return null;
    }

    @Override
    public CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema) {
        return null;
    }

    @Override
    public CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema, boolean force) {
        return null;
    }

    @Override
    public SchemaOpResult remove(String schemaName) {
        return null;
    }

    @Override
    public SchemaOpResult remove(String schemaName, boolean force) {
        return null;
    }

    @Override
    public CompletionStage<SchemaOpResult> removeAsync(String schemaName) {
        return null;
    }

    @Override
    public CompletionStage<SchemaOpResult> removeAsync(String schemaName, boolean force) {
        return null;
    }

    @Override
    public boolean exists(String schemaName) {
        return false;
    }

    @Override
    public Optional<String> retrieveError(String schemaName) {
        return Optional.empty();
    }

    @Override
    public CompletionStage<Optional<String>> retrieveErrorAsync(String schemaName) {
        return null;
    }

    @Override
    public SchemaErrors retrieveAllSchemaErrors() {
        return null;
    }

    @Override
    public CompletionStage<SchemaErrors> retrieveAllSchemaErrorsAsync() {
        return null;
    }
}
