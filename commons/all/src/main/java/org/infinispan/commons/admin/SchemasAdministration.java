package org.infinispan.commons.admin;

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
public interface SchemasAdministration {
    /**
     * Gets the schema by name.
     *
     * @param schemaName the name of the schema
     * @return optional schema
     */
    Optional<Schema> get(String schemaName);

    /**
     * Non-blocking version of {@link SchemasAdministration#get(String)}
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
     * Non-blocking version of {@link SchemasAdministration#retrieveError(String)}
     */
    CompletionStage<Optional<String>> retrieveErrorAsync(String schemaName);

    /**
     * Retrieves all schema errors.
     *
     * @return all schema errors
     */
    SchemaErrors retrieveAllSchemaErrors();

    /**
     * Non-blocking version of {@link SchemasAdministration#retrieveAllSchemaErrors()}
     */
    CompletionStage<SchemaErrors> retrieveAllSchemaErrorsAsync();

    /**
     * Creates a new schema. If the schema does not exist, it does nothing and
     * {@link SchemaOpResult#isCreated()} returns false.
     *
     * @param schema the schema to create
     * @return result of the operation
     */
    SchemaOpResult create(Schema schema);

    /**
     * Non-blocking version of {@link SchemasAdministration#create(Schema)}
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
     * Non-blocking version of {@link SchemasAdministration#update(Schema)}
     */
    CompletionStage<SchemaOpResult> updateAsync(Schema schema);

    /**
     * Non-blocking version of {@link SchemasAdministration#update(Schema, boolean)}
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
     * Non-blocking version of {@link SchemasAdministration#createOrUpdate(Schema)}
     */
    CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema);

    /**
     * Non-blocking version of {@link SchemasAdministration#createOrUpdate(Schema, boolean)}
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
     * Non-blocking version of {@link SchemasAdministration#remove(String)}
     */
    CompletionStage<SchemaOpResult> removeAsync(String schemaName);

    /**
     * Non-blocking version of {@link SchemasAdministration#remove(String, boolean)}
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
    class SchemaOpResult {
        public enum OperationType {
            CREATE,
            UPDATE,
            DELETE,
            NONE
        }

        private final OperationType operationType;

        private SchemaOpResult(Builder builder) {
            this.operationType = builder.operationType;
        }

        public OperationType getOperationType() {
            return operationType;
        }

        public boolean isCreated() {
            return operationType == OperationType.CREATE;
        }

        public boolean isNone() {
            return operationType == OperationType.NONE ;
        }

        public boolean isUpdated() {
            return operationType == OperationType.UPDATE;
        }

        public boolean isDeleted() {
            return operationType == OperationType.DELETE;
        }

        @Override
        public String toString() {
            return "SchemaOpResult{" +
                    "operationType=" + operationType +
                    '}';
        }

        public static class Builder {
            private OperationType operationType = OperationType.NONE;

            public Builder operationType(OperationType operationType) {
                this.operationType = operationType;
                return this;
            }

            public SchemaOpResult build() {
                return new SchemaOpResult(this);
            }
        }

        public static SchemaOpResult created() {
            return new Builder()
                    .operationType(OperationType.CREATE)
                    .build();
        }

        public static SchemaOpResult updated() {
            return new Builder()
                    .operationType(OperationType.UPDATE)
                    .build();
        }

        public static SchemaOpResult deleted() {
            return new Builder()
                    .operationType(OperationType.DELETE)
                    .build();
        }

        public static SchemaOpResult none() {
            return new Builder()
                    .operationType(OperationType.NONE)
                    .build();
        }

        public static Builder builder() {
            return new Builder();
        }
    }
}
