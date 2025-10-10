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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.infinispan.client.hotrod.Flag.FORCE_RETURN_VALUE;
import static org.infinispan.client.hotrod.impl.RemoteCacheManagerAdminImpl.string;

public class RemoteSchemasAdminImpl implements RemoteSchemasAdmin {
    public static final Log log = LogFactory.getLog(RemoteSchemasAdminImpl.class, Log.class);

    private final ManagerOperationsFactory operationsFactory;
    private final OperationDispatcher operationDispatcher;
    private final RemoteCache<String, String> protostreamCache;
    private static final byte[] CREATE = string("c");
    private static final byte[] UPDATE = string("u");
    private static final byte[] SAVE = string("s");
    private static final byte[] FORCE = string(Boolean.toString(true));

    protected RemoteSchemasAdminImpl(ManagerOperationsFactory operationsFactory, OperationDispatcher operationDispatcher, RemoteCacheManager cacheManager) {
        this.operationsFactory = operationsFactory;
        this.operationDispatcher = operationDispatcher;
        this.protostreamCache = cacheManager.<String, String>getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME).withFlags(FORCE_RETURN_VALUE);
    }

   @Override
   public CompletionStage<Boolean> existsAsync(String schemaName) {
      return protostreamCache.containsKeyAsync(schemaName);
   }

   @Override
   public CompletionStage<Optional<Schema>> getAsync(String schemaName) {
      return protostreamCache.getWithMetadataAsync(schemaName)
            .thenApply(e -> getSchemaContent(schemaName, e))
            .exceptionally(ex -> {
               log.errorf("Error raised when getting schema %s", schemaName, ex);
               throw new InfinispanException(ex.getMessage(), ex);
            });
   }

    @Override
    public CompletionStage<SchemaOpResult> createAsync(Schema schema) {
       return callCreateOrUpdateSchema(schema, CREATE, false);
    }

   @Override
   public CompletionStage<Map<String, SchemaOpResult>> createAsync(FileDescriptorSource fileDescriptorSource) {
      return createOrUpdateFileDescriptors(fileDescriptorSource, schema -> createAsync(schema));
   }

   @Override
    public CompletionStage<SchemaOpResult> updateAsync(Schema schema, boolean force) {
       return callCreateOrUpdateSchema(schema, UPDATE, force);
    }

   @Override
   public CompletionStage<Map<String, SchemaOpResult>> updateAsync(FileDescriptorSource fileDescriptorSource) {
      return createOrUpdateFileDescriptors(fileDescriptorSource, schema -> updateAsync(schema));
   }

   @Override
   public CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema, boolean force) {
      return callCreateOrUpdateSchema(schema, SAVE, force);
   }

   @Override
   public CompletionStage<Map<String, SchemaOpResult>> createOrUpdateAsync(FileDescriptorSource fileDescriptorSource) {
      return createOrUpdateFileDescriptors(fileDescriptorSource, schema -> createOrUpdateAsync(schema));
   }

   private CompletableFuture<Map<String, SchemaOpResult>> createOrUpdateFileDescriptors(FileDescriptorSource fileDescriptorSource,
                                                                                        Function<Schema, CompletionStage<SchemaOpResult>> schemaOp) {
      Map<String, CompletionStage<SchemaOpResult>> results = new HashMap<>(fileDescriptorSource.getFiles().size());

      fileDescriptorSource.getFiles().entrySet().forEach(entry ->
            results.put(entry.getKey(), schemaOp.apply(Schema.buildFromStringContent(entry.getKey(), entry.getValue())
            )));

      CompletableFuture<Void> allDone = CompletableFuture.allOf(results.values().toArray(new CompletableFuture[0]));

      return allDone.thenApply(v -> results.entrySet().stream()
            .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> e.getValue().toCompletableFuture().join()
            )));
   }

   @Override
   public CompletionStage<SchemaOpResult> removeAsync(String name, boolean force) {
      if (force) {
         return protostreamCache.withFlags(FORCE_RETURN_VALUE)
               .removeAsync(name).thenApply(r -> {
                  if (r == null) {
                     log.infof(String.format("Schema %s not found", name));
                     return new SchemaOpResult(SchemaOpResultType.NONE);
                  }
                  return new SchemaOpResult(SchemaOpResultType.DELETED);
               });
      }

      Map<String, byte[]> params = new HashMap<>(1);
      params.put("name", string(name));
      return operationDispatcher
            .execute(operationsFactory.executeOperation("@@schemas@delete", params))
            .thenApply(r -> new SchemaOpResult(getSchemaOpResult(name, r)));
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
   public CompletionStage<Optional<String>> retrieveErrorAsync(String schemaName) {
      if (log.isDebugEnabled()) {
         return CompletionStages.handleAndCompose(protostreamCache.getAsync(schemaName), (schemaContent, ex) -> {
            if (ex != null) {
               log.errorf("Exception raised getting schema %s", schemaName, ex);
               throw new InfinispanException(ex.getMessage(), ex);
            }

            if (schemaContent == null) {
               log.debugf(String.format("Schema %s not found", schemaName));
               return CompletableFuture.completedFuture(Optional.empty());
            }
            return getErrorAsync(schemaName);
         });
      }

      return getErrorAsync(schemaName);
   }

   private CompletionStage<SchemaOpResult> callCreateOrUpdateSchema(Schema schema, byte[] op, boolean force) {
      Map<String, byte[]> params = new HashMap<>(4);
      params.put("name", string(schema.getName()));
      params.put("content", string(schema.getContent()));
      params.put("op", op);
      if (force) {
         params.put("force", FORCE);
      }
      return operationDispatcher
            .execute(operationsFactory.executeOperation("@@schemas@createOrUpdate", params))
            .thenApply(opResult -> createOrUpdateToSchemaOpResult(opResult))
            .exceptionally(ex -> {
               log.errorf("Error raised with schema %s", schema.getName(), ex);
               throw new InfinispanException(ex.getMessage(), ex);
            });
   }

    private static Optional<Schema> getSchemaContent(String schemaName, MetadataValue<String> withMetadata) {
        if (withMetadata != null && withMetadata.getValue() != null) {
            return Optional.of(Schema.buildFromStringContent(schemaName, withMetadata.getValue()));
        }
        return Optional.empty();
    }

   private static SchemaOpResult createOrUpdateToSchemaOpResult(String opResult) {
      char c = opResult.charAt(0);
      SchemaOpResultType schemaOpResultType = SchemaOpResultType.fromCode(c);

      if (opResult.length() == 1) {
         // No error
         return new SchemaOpResult(schemaOpResultType);
      }

      // Get the error
      String error = opResult.substring(2);
      return new SchemaOpResult(schemaOpResultType, error);
   }

    private static SchemaOpResultType getSchemaOpResult(String schemaName, String opResult) {
        int safeDelete = Integer.parseInt(opResult);
        if (safeDelete < 0) {
            throw new InfinispanException(String.format("A cache is using one entity in the schema %s. Remove not done", schemaName));
        }

        if (safeDelete == 1) {
            return SchemaOpResultType.DELETED;
        }

        log.infof(String.format("Schema %s not found", schemaName));
        return SchemaOpResultType.NONE;
    }

    private CompletionStage<Optional<String>> getErrorAsync(String schemaName) {
       return protostreamCache.getAsync(schemaName + SchemaErrors.ERRORS_KEY_SUFFIX)
             .thenApply(error -> Optional.ofNullable(error))
             .exceptionally(ex -> {
                log.errorf("Exception raised getting schema error %s", schemaName, ex);
                throw new InfinispanException(ex.getMessage(), ex);
             });
    }
}
