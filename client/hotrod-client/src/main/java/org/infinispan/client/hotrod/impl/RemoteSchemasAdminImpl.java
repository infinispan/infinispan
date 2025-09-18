package org.infinispan.client.hotrod.impl;

import org.infinispan.api.exception.InfinispanException;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.client.hotrod.impl.operations.ManagerOperationsFactory;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.schema.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static org.infinispan.client.hotrod.Flag.FORCE_RETURN_VALUE;
import static org.infinispan.client.hotrod.impl.RemoteCacheManagerAdminImpl.string;
import static org.infinispan.client.hotrod.impl.Util.await;

public class RemoteSchemasAdminImpl implements RemoteSchemasAdmin {
    public static final Log log = LogFactory.getLog(RemoteSchemasAdminImpl.class, Log.class);

    private final ManagerOperationsFactory operationsFactory;
    private final OperationDispatcher operationDispatcher;
    private final RemoteCache<String, String> protostreamCache;
    private final int timeout;

    protected RemoteSchemasAdminImpl(ManagerOperationsFactory operationsFactory, OperationDispatcher operationDispatcher, RemoteCacheManager cacheManager) {
        this.operationsFactory = operationsFactory;
        this.operationDispatcher = operationDispatcher;
        this.protostreamCache = cacheManager.<String, String>getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME).withFlags(FORCE_RETURN_VALUE);
        this.timeout = protostreamCache.getRemoteCacheContainer().getConfiguration().connectionTimeout();
    }

    @Override
    public SchemaOpResult create(Schema schema) {
        return create(schema.getName(), schema.getContent());
    }

    @Override
    public Map<String, SchemaOpResult> create(FileDescriptorSource generatedSchema) {
        Map<String, SchemaOpResult> results = new HashMap<>(generatedSchema.getFiles().size());
        generatedSchema.getFiles().entrySet().forEach(entry -> {
            results.put(entry.getKey(), create(entry.getKey(), entry.getValue()));
        });

        return results;
    }

    private SchemaOpResult create(String key, String schemaContent) {
        var result = protostreamCache.putIfAbsent(key, schemaContent);
        return result == null ? SchemaOpResult.DELETED : SchemaOpResult.NONE;
    }

    @Override
    public CompletionStage<SchemaOpResult> createAsync(Schema schema) {
        return protostreamCache.putIfAbsentAsync(schema.getName(), schema.getContent())
                .thenApply(r -> r == null ? SchemaOpResult.CREATED : SchemaOpResult.NONE);
    }

    @Override
    public SchemaOpResult update(Schema schema) {
        return update(schema, false);
    }

    @Override
    public SchemaOpResult update(Schema schema, boolean force) {
        if (force) {
            // replace without any check
            String prevValue = protostreamCache.put(schema.getName(), schema.getContent());
            return prevValue != null ? SchemaOpResult.UPDATED : SchemaOpResult.NONE;
        }

        var stored = protostreamCache.getWithMetadata(schema.getName());
        boolean updateResult = false;
        if (stored != null && !Objects.equals(stored.getValue(), schema.getContent())) {
            if (protostreamCache.replaceWithVersion(schema.getName(), schema.getContent(), stored.getVersion())) {
                updateResult = true;
            }
        }
        return updateResult ? SchemaOpResult.UPDATED : SchemaOpResult.NONE;
    }

    @Override
    public CompletionStage<SchemaOpResult> updateAsync(Schema schema) {
        return updateAsync(schema, false);
    }

    @Override
    public CompletionStage<SchemaOpResult> updateAsync(Schema schema, boolean force) {
        if (force) {
            // if force is true, replace without any check
            return protostreamCache.putAsync(schema.getName(), schema.getContent())
                    .thenApply(prevValue -> prevValue != null ? SchemaOpResult.UPDATED : SchemaOpResult.NONE)
                    .exceptionally(ex -> {
                        if (ex != null) {
                            log.errorf("Error raised when updating schema %s", schema.getName(), ex);
                            throw new InfinispanException(ex.getMessage(), ex);
                        }
                        return SchemaOpResult.NONE;
                    });
        }

        return CompletionStages.handleAndCompose(protostreamCache.getWithMetadataAsync(schema.getName()),
                (stored, ex) -> {
                    if (ex != null) {
                        log.errorf("Error raised when updating schema %s", schema.getName(), ex);
                        throw new InfinispanException(ex.getMessage(), ex);
                    }

                    if (stored == null) {
                        // If it does not exist, there is  nothing to be updated
                        return CompletableFuture.completedFuture(SchemaOpResult.NONE);
                    }

                    if (stored != null && !Objects.equals(stored.getValue(), schema.getContent())) {
                        return protostreamCache.replaceWithVersionAsync(schema.getName(), schema.getContent(), stored.getVersion())
                                .thenApply(r -> r ? SchemaOpResult.UPDATED : SchemaOpResult.NONE)
                                .exceptionally(ex2 -> {
                                   log.errorf("Error raised when updating schema %s", schema.getName(), ex2);
                                   throw new InfinispanException(ex2.getMessage(), ex2);
                                });
                    }
                    // Nothing has been done
                    return CompletableFuture.completedFuture(SchemaOpResult.NONE);
                });
    }

    @Override
    public SchemaOpResult createOrUpdate(Schema schema) {
        return createOrUpdate(schema, false);
    }

    @Override
    public SchemaOpResult createOrUpdate(Schema schema, boolean force) {
        if (force) {
            // Update regardless of the actual value
            String prev = protostreamCache.put(schema.getName(), schema.getContent());
            return prev == null ? SchemaOpResult.CREATED : SchemaOpResult.UPDATED;
        }

        MetadataValue<String> actualValue = protostreamCache.getWithMetadata(schema.getName());
        if (actualValue == null) {
            // create the value, if such exists. Might not create it if meanwhile other call created it
            return create(schema);
        }
        // Update. might not update anything if other process updated it before
        return update(schema);
    }


    @Override
    public Map<String, SchemaOpResult> createOrUpdate(FileDescriptorSource fileDescriptorSource) {
        Map<String, String> files = fileDescriptorSource.getFiles();
        Map<String, SchemaOpResult> results = new HashMap<>(files.size());
        for (Map.Entry<String, String> entry : files.entrySet()) {
            results.put(entry.getKey(), createOrUpdate(Schema.buildFromStringContent(entry.getKey(), entry.getValue())));
        }
        return results;
    }

    @Override
    public CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema) {
        return createOrUpdateAsync(schema, false);
    }

    @Override
    public CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema, boolean force) {
        if (force) {
            // Update regardless of the actual value
            return protostreamCache.putAsync(schema.getName(), schema.getContent())
                    .thenApply(r -> r == null ? SchemaOpResult.CREATED : SchemaOpResult.UPDATED)
                    .exceptionally(ex -> {
                        if (ex != null) {
                            throw new InfinispanException(ex.getMessage(), ex);
                        }
                        return SchemaOpResult.NONE;
                    });
        }

        return CompletionStages.handleAndCompose(protostreamCache.getWithMetadataAsync(schema.getName()), (actualSchema, ex) -> {
            if (ex != null) {
                throw new InfinispanException(ex.getMessage(), ex);
            }

            if (actualSchema == null) {
                // Might not create if other process creates meanwhile
                return createAsync(schema);
            }

            // Update. might not update anything if other process updates it before
            return updateAsync(schema);
        });
    }


    @Override
    public Optional<Schema> get(String schemaName) {
        MetadataValue<String> withMetadata = protostreamCache.getWithMetadata(schemaName);
        return getSchemaContent(schemaName, withMetadata);
    }

    @Override
    public CompletionStage<Optional<Schema>> getAsync(String schemaName) {
        return protostreamCache.getWithMetadataAsync(schemaName)
                .thenApply(e -> getSchemaContent(schemaName, e))
                .exceptionally(ex -> {
                    if (ex != null) {
                        throw new InfinispanException(ex.getMessage(), ex);
                    }
                    return Optional.empty();
                });
    }

    private static Optional<Schema> getSchemaContent(String schemaName, MetadataValue<String> withMetadata) {
        if (withMetadata != null && withMetadata.getValue() != null) {
            return Optional.of(Schema.buildFromStringContent(schemaName, withMetadata.getValue()));
        }

        return Optional.empty();
    }

    @Override
    public SchemaOpResult remove(String name) {
        return remove(name, false);
    }

    @Override
    public SchemaOpResult remove(String name, boolean force) {
        if (force) {
            String existing = protostreamCache.withFlags(FORCE_RETURN_VALUE).remove(name);
            if (existing == null) {
                log.infof(String.format("Schema %s not found", name));
            }
            return existing != null ? SchemaOpResult.DELETED : SchemaOpResult.NONE;
        }

        Map<String, byte[]> params = Map.of("schema", string(name));
        String opResult = await(operationDispatcher.execute(operationsFactory.executeOperation("@@schemas@delete", params)), timeout);
        return getSchemaOpResult(name, opResult);
    }

    @Override
    public CompletionStage<SchemaOpResult> removeAsync(String name) {
        return removeAsync(name, false);
    }

    @Override
    public CompletionStage<SchemaOpResult> removeAsync(String name, boolean force) {
        if (force) {
            return protostreamCache.withFlags(FORCE_RETURN_VALUE)
                    .removeAsync(name).thenApply(r -> {
                        if (r == null) {
                            log.infof(String.format("Schema %s not found", name));
                            return SchemaOpResult.NONE;
                        }
                        return SchemaOpResult.DELETED;
                    });
        }

        Map<String, byte[]> params = new HashMap<>(1);
        params.put("schema", string(name));
        return operationDispatcher
                .execute(operationsFactory.executeOperation("@@schemas@delete", params))
                .thenApply(r -> getSchemaOpResult(name, r));
    }

    private static SchemaOpResult getSchemaOpResult(String schemaName, String opResult) {
        int safeDelete = Integer.parseInt(opResult);
        if (safeDelete < 0) {
            throw new InfinispanException(String.format("A cache is using one entity in the schema %s. Remove not done", schemaName));
        }

        if (safeDelete == 1) {
            return SchemaOpResult.DELETED;
        }

        log.infof(String.format("Schema %s not found", schemaName));
        return SchemaOpResult.NONE;
    }

    @Override
    public SchemaErrors retrieveAllSchemaErrors() {
        SchemaErrors schemaErrors = new SchemaErrors();
        String errors = protostreamCache.get(SchemaErrors.ERRORS_KEY_SUFFIX);
        if (errors == null) {
            return schemaErrors;
        }
        Set<String> errorFileKeys = Set.of(errors.split("\n"))
                .stream()
                .map(errorFile -> errorFile + SchemaErrors.ERRORS_KEY_SUFFIX)
                .collect(Collectors.toSet());

        Map<String, String> mapErrors = protostreamCache.getAll(errorFileKeys);
        schemaErrors.addAll(mapErrors);
        return schemaErrors;
    }

    @Override
    public CompletionStage<SchemaErrors> retrieveAllSchemaErrorsAsync() {
        return CompletionStages.handleAndCompose(
                protostreamCache.getAsync(SchemaErrors.ERRORS_KEY_SUFFIX),
                (errors, ex) -> {
                    SchemaErrors schemaErrors = new SchemaErrors();
                    if (ex != null) {
                        log.error(ex.getMessage(), ex);
                    } else if (errors != null) {
                        Set<String> errorFileKeys = Set.of(errors.split("\n"))
                                .stream()
                                .map(errorFile -> errorFile + SchemaErrors.ERRORS_KEY_SUFFIX)
                                .collect(Collectors.toSet());

                        return protostreamCache.getAllAsync(errorFileKeys)
                                .thenApply((mapErrors) -> {
                                    schemaErrors.addAll(mapErrors);
                                    return schemaErrors;
                                }).exceptionally(ex2 -> {
                                    log.error(ex2.getMessage(), ex2);
                                    return schemaErrors;
                                });
                    }
                    return CompletableFuture.completedFuture(schemaErrors);
                });
    }

    @Override
    public boolean exists(String schemaName) {
        return protostreamCache.containsKey(schemaName);
    }

    @Override
    public Optional<String> retrieveError(String schemaName) {
        if (log.isDebugEnabled()) {
           String schema = protostreamCache.get(schemaName);
           if (schema == null) {
              log.debugf(String.format("Schema %s not found", schemaName));
              return Optional.empty();
           }
        }

        String error = protostreamCache.get(schemaName + SchemaErrors.ERRORS_KEY_SUFFIX);
        return Optional.ofNullable(error);
    }

    @Override
    public CompletionStage<Optional<String>> retrieveErrorAsync(String schemaName) {
       if (log.isDebugEnabled()) {
          return CompletionStages.handleAndCompose(protostreamCache.getAsync(schemaName), (schema, ex) -> {
             if (ex != null) {
                log.errorf("Exception raised getting schema %s", schemaName, ex);
                throw new InfinispanException(ex.getMessage(), ex);
             }

             if (schema == null) {
                log.debugf(String.format("Schema %s not found", schemaName));
                return CompletableFuture.completedFuture(Optional.empty());
             }
             return getErrorAsync(schema);
          });
       }

       return getErrorAsync(schemaName);
    }

    private CompletionStage<Optional<String>> getErrorAsync(String schemaName) {
       return protostreamCache.getAsync(schemaName + SchemaErrors.ERRORS_KEY_SUFFIX)
             .thenApply(error -> Optional.ofNullable(error))
             .exceptionally(ex -> {
                if (ex != null) {
                   log.errorf("Exception raised getting schema error %s", schemaName, ex);
                   throw new InfinispanException(ex.getMessage(), ex);
                }
                return Optional.empty();
             });
    }
}
