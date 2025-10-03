package org.infinispan.client.hotrod;

import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.schema.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Interface for managing schema operations such as create, update, delete, and retrieve.
 *
 * @since 16.0
 * @author Katia Aresti
 */
public interface RemoteSchemasAdmin {
    /**
     * Gets the schema by name.
     *
     * @param schemaName the name of the schema
     * @return optional schema
     */
    Optional<Schema> get(String schemaName);

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#get(String)}
     */
    CompletionStage<Optional<Schema>> getAsync(String schemaName);

    /**
     * Gets the schema error by name, if such exists.
     *
     * @param schemaName the name of the schema
     * @return optional schema error
     */
    Optional<String> retrieveError(String schemaName);

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#retrieveError(String)}
     */
    CompletionStage<Optional<String>> retrieveErrorAsync(String schemaName);

    /**
     * Retrieves all schema errors.
     *
     * @return all schema errors
     */
    SchemaErrors retrieveAllSchemaErrors();

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#retrieveAllSchemaErrors()}
     */
    CompletionStage<SchemaErrors> retrieveAllSchemaErrorsAsync();

    /**
     * Creates a new schema. If the schema does not exist, it does nothing and
     * {@link SchemaOpResult#CREATED} returns false.
     *
     * @param schema the schema to create
     * @return result of the operation
     */
    SchemaOpResult create(Schema schema);

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#create(Schema)}
     */
    CompletionStage<SchemaOpResult> createAsync(Schema schema);

    /**
     * Creates schemas from descriptor source. Creates 0 to {@link FileDescriptorSource#getFiles()}
     * size schemas.
     *
     * @param fileDescriptorSource source of schema descriptors
     * @return map of schema names to results
     */
    Map<String, SchemaOpResult> create(FileDescriptorSource fileDescriptorSource);

    /**
     * Updates an existing schema.
     *
     * @param schema the schema to update
     * @return result of the operation
     */
    SchemaOpResult update(Schema schema);

    /**
     * Updates an existing schema, if it does not exist.
     * If force is false, bypasses version checking.
     *
     * @param schema the schema to update
     * @return result of the operation
     */
    SchemaOpResult update(Schema schema, boolean force);

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#update(Schema)}
     */
    CompletionStage<SchemaOpResult> updateAsync(Schema schema);

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#update(Schema, boolean)}
     */
    CompletionStage<SchemaOpResult> updateAsync(Schema schema, boolean force);

    /**
     * Creates or updates a schema.
     *
     * @param schema the schema to create or update
     * @return result of the operation
     */
    SchemaOpResult createOrUpdate(Schema schema);

    /**
     * Creates or updates a schema, regardless of the version of the schema.
     *
     * @param schema the schema to create or update
     * @return result of the operation
     */
    SchemaOpResult createOrUpdate(Schema schema, boolean force);

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#createOrUpdate(Schema)}
     */
    CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema);

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#createOrUpdate(Schema, boolean)}
     */
    CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema, boolean force);

    /**
     * Creates or updates schemas from descriptor source.
     *
     * @param fileDescriptorSource source of schema descriptors
     * @return map of schema names to results
     */
    Map<String, SchemaOpResult> createOrUpdate(FileDescriptorSource fileDescriptorSource);

    /**
     * Deletes a schema.
     *
     * @param schemaName the name of the schema
     * @return result of the operation
     */
    SchemaOpResult remove(String schemaName);

    /**
     * Deletes a schema, with option to force deletion.
     *
     * @param schemaName the name of the schema
     * @param force whether to force delete
     * @return result of the operation
     */
    SchemaOpResult remove(String schemaName, boolean force);

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#remove(String)}
     */
    CompletionStage<SchemaOpResult> removeAsync(String schemaName);

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#remove(String, boolean)}
     */
    CompletionStage<SchemaOpResult> removeAsync(String schemaName, boolean force);

    /**
     * Checks if a schema exists.
     *
     * @param schemaName the name of the schema
     * @return true if exists, false otherwise
     */
    boolean exists(String schemaName);

    record SchemaError(String fileName, String error) {}

    class SchemaErrors {
        private Map<String, String> errors = new HashMap<>();
        public static final String ERRORS_KEY_SUFFIX = ".errors";

        public String getError(String schemaName) {
            Objects.requireNonNull(schemaName);
            return errors != null ? errors.getOrDefault(schemaName + ERRORS_KEY_SUFFIX, "") : "";
        }

        public void addAll(Map<String, String> mapErrors) {
            errors.putAll(mapErrors);
        }

        public boolean isEmpty() {
            return errors.isEmpty();
        }

        public Map<String, String> allErrorAsMap() {
            return errors;
        }

        public List<SchemaError> allErrorsAsList() {
            return errors.entrySet().stream().map(e -> new SchemaError(e.getKey(), e.getValue())).collect(Collectors.toList());
        }

        public Set<String> files() {
            return errors.keySet();
        }
    }

    // Holds operation result. Does not catch errors (for now)
    enum SchemaOpResult {
       CREATED,
       UPDATED,
       DELETED,
       NONE
    }
}
