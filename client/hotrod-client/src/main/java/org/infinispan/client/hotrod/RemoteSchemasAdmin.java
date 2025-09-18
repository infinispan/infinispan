package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.schema.Schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Manages schema operations such as create, update, delete, and retrieval.
 * <p>
 *
 * Marked as {@link Experimental} as it may change.
 *
 * @since 16.0
 */
@Experimental
public interface RemoteSchemasAdmin {
    /**
     * Returns the schema by name.
     *
     * @param schemaName schema name
     * @return optional schema
     */
    default Optional<Schema> get(String schemaName) {
       return getAsync(schemaName).toCompletableFuture().join();
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#get(String)}
     */
    CompletionStage<Optional<Schema>> getAsync(String schemaName);

    /**
     * Returns the schema error, if present.
     *
     * @param schemaName schema name
     * @return optional schema error
     */
    default Optional<String> retrieveError(String schemaName) {
       return retrieveErrorAsync(schemaName).toCompletableFuture().join();
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#retrieveError(String)}
     */
    CompletionStage<Optional<String>> retrieveErrorAsync(String schemaName);

    /**
     * Returns all schema errors.
     *
     * @return all schema errors
     */
    default SchemaErrors retrieveAllSchemaErrors() {
       return retrieveAllSchemaErrorsAsync().toCompletableFuture().join();
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#retrieveAllSchemaErrors()}
     */
    CompletionStage<SchemaErrors> retrieveAllSchemaErrorsAsync();

    /**
     * Creates a new schema.
     *
     * @param schema the schema to create
     * @return {@link SchemaOpResult} containing the op result {@link SchemaOpResultType#CREATED}, {@link SchemaOpResultType#NONE}
     * and the schema validation error, if such exists
     */
    default SchemaOpResult create(Schema schema) {
       return createAsync(schema).toCompletableFuture().join();
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#create(Schema)}
     */
    CompletionStage<SchemaOpResult> createAsync(Schema schema);

    /**
     * Creates schemas from a descriptor source.
     *
     * @param fileDescriptorSource descriptor source
     * @return map of schema names to results
     */
    default Map<String, SchemaOpResult> create(FileDescriptorSource fileDescriptorSource) {
       return createAsync(fileDescriptorSource).toCompletableFuture().join();
    }

   /**
    * Non-blocking version of {@link RemoteSchemasAdmin#create(FileDescriptorSource)}
    */
    CompletionStage<Map<String, SchemaOpResult>> createAsync(FileDescriptorSource fileDescriptorSource);

    /**
     * Updates an existing schema.
     * Retrieves the current schema value. If the schema has changed by the time the update
     * occurs, the update is skipped. This prevents overwriting changes made by another node.
     *
     * @param schema the schema to update
     * @return {@link SchemaOpResult} containing the op result {@link SchemaOpResultType#UPDATED}, {@link SchemaOpResultType#NONE}
     * and the schema validation error, if such exists
     */
    default SchemaOpResult update(Schema schema) {
       return update(schema, false);
    }

   /**
    * Updates an existing schema with optional force.
    * If force=false, retrieves the current schema value. If the schema has changed by the time the update
    * occurs, the update is skipped. This prevents overwriting changes made by another node.
    * If force=true, the update is done on every case.
    *
    * @param schema the schema to update
    * @param force, if true checks the version of the schema
    * @return {@link SchemaOpResult} containing the op result {@link SchemaOpResultType#UPDATED}, {@link SchemaOpResultType#NONE}
    * and the schema validation error, if such exists
    */
    default SchemaOpResult update(Schema schema, boolean force) {
       return updateAsync(schema, force).toCompletableFuture().join();
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#update(Schema)}
     */
    default CompletionStage<SchemaOpResult> updateAsync(Schema schema) {
       return updateAsync(schema, false);
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#update(Schema, boolean)}
     */
    CompletionStage<SchemaOpResult> updateAsync(Schema schema, boolean force);

   /**
    * Updates schemas from a descriptor source.
    *
    * @param fileDescriptorSource descriptor source
    * @return map of schema names to results
    */
   default Map<String, SchemaOpResult> update(FileDescriptorSource fileDescriptorSource) {
      return updateAsync(fileDescriptorSource).toCompletableFuture().join();
   }

   /**
    * Non-blocking version of {@link RemoteSchemasAdmin#update(FileDescriptorSource)}
    */
   CompletionStage<Map<String, SchemaOpResult>> updateAsync(FileDescriptorSource fileDescriptorSource);

    /**
     * Creates or updates a schema.
     *
     * @param schema the schema to create or update
     * @return {@link SchemaOpResult} containing the op result  {@link SchemaOpResultType#CREATED}, {@link SchemaOpResultType#UPDATED}, or {@link SchemaOpResultType#NONE}
     * and the schema validation error, if such exists
     */
    default SchemaOpResult createOrUpdate(Schema schema) {
       return createOrUpdate(schema, false);
    }

   /**
    * Creates or updates a schema.
    * If force=false, retrieves the current schema value. If the schema has changed by the time the operation
    * occurs, the creation or the update is skipped. This prevents overwriting changes made by another node.
    * If force=true, the creation or the update is done on every case.
    *
    * @param schema the schema to create or update
    * @param force force the schema create or update
    * @return {@link SchemaOpResult} containing the op result  {@link SchemaOpResultType#CREATED}, {@link SchemaOpResultType#UPDATED}, or {@link SchemaOpResultType#NONE}
    * and the schema validation error, if such exists
    */
    default SchemaOpResult createOrUpdate(Schema schema, boolean force) {
       return createOrUpdateAsync(schema, force).toCompletableFuture().join();
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#createOrUpdate(Schema)}
     */
    default CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema) {
       return createOrUpdateAsync(schema, false);
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#createOrUpdate(Schema, boolean)}
     */
    CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema, boolean force);

   /**
    * Non-blocking version of {@link RemoteSchemasAdmin#createOrUpdate(FileDescriptorSource)}
    */
    default Map<String, SchemaOpResult> createOrUpdate(FileDescriptorSource fileDescriptorSource) {
       return createOrUpdateAsync(fileDescriptorSource).toCompletableFuture().join();
    }

   /**
    * Creates or updates schemas from descriptor source.
    *
    * @param fileDescriptorSource source of schema descriptors
    * @return map of schema names to results
    */
    CompletionStage<Map<String, SchemaOpResult>> createOrUpdateAsync(FileDescriptorSource fileDescriptorSource);

    /**
     * Deletes a schema. Does not force deletion.
     * If the method is invoked while a cache already holds a reference to a schema entity,
     * an exception is thrown.
     *
     * @param schemaName schema name
     * @return {@link SchemaOpResultType#DELETED} if deleted, {@link SchemaOpResultType#NONE} if not found
     * @throws {@link org.infinispan.api.exception.InfinispanException} if deletion is blocked by cache dependencies
     */
    default SchemaOpResult remove(String schemaName) {
       return remove(schemaName, false);
    }

    /**
     * Deletes a schema with optional force.
     * If the method is invoked while a cache already holds a reference to a schema entity,
     * an exception is thrown if force=false. Setting force=true bypasses this check
     * and deletes the schema, if such exists.
     *
     * @param schemaName schema name
     * @param force      if true, bypasses dependency checks
     * @return {@link SchemaOpResultType#DELETED} if deleted, {@link SchemaOpResultType#NONE} if not found
     * @throws {@link org.infinispan.api.exception.InfinispanException} if not forced and schema is in use
     */
    default SchemaOpResult remove(String schemaName, boolean force) {
       return removeAsync(schemaName, force).toCompletableFuture().join();
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#remove(String)}
     */
    default CompletionStage<SchemaOpResult> removeAsync(String schemaName) {
       return removeAsync(schemaName, false);
    }

    /**
     * Non-blocking version of {@link RemoteSchemasAdmin#remove(String, boolean)}
     */
    CompletionStage<SchemaOpResult> removeAsync(String schemaName, boolean force);

    /**
     * Checks if the schema exists.
     *
     * @param schemaName schema name
     * @return true if exists, false otherwise
     */
    default boolean exists(String schemaName) {
       return existsAsync(schemaName).toCompletableFuture().join();
    }

   /**
    * Non-blocking version of {@link RemoteSchemasAdmin#exists(String)}
    */
    CompletionStage<Boolean> existsAsync(String schemaName);

   /** Holds a schema name and its associated error message. */
    record SchemaError(String name, String error) {}

   /**
    * For multiple schema errors.
    */
    record SchemaErrors(Map<String, String> errors) {
        public static final String ERRORS_KEY_SUFFIX = ".errors";

        public SchemaErrors {
           // Ensure errors is never null and is immutable
           errors = errors == null ? Map.of() : Map.copyOf(errors);
        }

        public String getError(String schemaName) {
           Objects.requireNonNull(schemaName);
           return errors.getOrDefault(schemaName + ERRORS_KEY_SUFFIX, "");
        }

        public boolean isEmpty() {
            return errors.isEmpty();
        }

        public List<SchemaError> allErrorsAsList() {
            return errors.entrySet().stream().map(e -> new SchemaError(e.getKey(), e.getValue())).collect(Collectors.toList());
        }

        public Set<String> schemas() {
            return errors.keySet();
        }
    }

    // Holds operation result. Does not catch errors (for now)
    enum SchemaOpResultType {
       CREATED,
       UPDATED,
       DELETED,
       NONE,
       ERROR;

       public static SchemaOpResultType fromCode(char c) {
          if (c == 'c') {
             return CREATED;
          }

          if (c == 'u') {
             return UPDATED;
          }

          if (c == 'd') {
             return DELETED;
          }

          if (c == 'n') {
             return NONE;
          }

          return ERROR;
       }
    }

    class SchemaOpResult {
       SchemaOpResultType type = SchemaOpResultType.NONE;
       String error = "";

       public SchemaOpResult() {

       }
       public SchemaOpResult(SchemaOpResultType type, String error) {
          this.type = type;
          this.error = error;
       }

       public SchemaOpResult(SchemaOpResultType type) {
          this.type = type;
       }

       public static SchemaOpResult none() {
          return new SchemaOpResult();
       }

       public SchemaOpResultType getType() {
          return type;
       }

       public String getError() {
          return error;
       }

       public boolean hasError() {
          return !error.isEmpty();
       }
    }

}
