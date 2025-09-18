package org.infinispan.client.hotrod.impl;

import org.infinispan.api.exception.InfinispanException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private static final byte[] FORCE = string("f");

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
      return protostreamCache.getAsync(schemaName)
            .thenApply(content -> getSchemaContent(schemaName, content))
            .exceptionally(ex -> {
               log.crudSchemaError(schemaName, ex);
               throw new InfinispanException(ex);
            });
   }

    @Override
    public CompletionStage<SchemaOpResult> createAsync(Schema schema) {
       return callCreateOrUpdateSchema(schema, CREATE, false);
    }

   @Override
   public CompletionStage<Map<String, SchemaOpResult>> createAsync(FileDescriptorSource fileDescriptorSource) {
      return createOrUpdateFileDescriptors(fileDescriptorSource, this::createAsync);
   }

   @Override
    public CompletionStage<SchemaOpResult> updateAsync(Schema schema, boolean force) {
       return callCreateOrUpdateSchema(schema, UPDATE, force);
    }

   @Override
   public CompletionStage<Map<String, SchemaOpResult>> updateAsync(FileDescriptorSource fileDescriptorSource) {
      return createOrUpdateFileDescriptors(fileDescriptorSource, this::updateAsync);
   }

   @Override
   public CompletionStage<SchemaOpResult> createOrUpdateAsync(Schema schema, boolean force) {
      return callCreateOrUpdateSchema(schema, SAVE, force);
   }

   @Override
   public CompletionStage<Map<String, SchemaOpResult>> createOrUpdateAsync(FileDescriptorSource fileDescriptorSource) {
      return createOrUpdateFileDescriptors(fileDescriptorSource, this::createOrUpdateAsync);
   }

   private CompletableFuture<Map<String, SchemaOpResult>> createOrUpdateFileDescriptors(FileDescriptorSource fileDescriptorSource,
                                                                                        Function<Schema, CompletionStage<SchemaOpResult>> schemaOp) {
      var files = fileDescriptorSource.getFiles();
      Map<String, SchemaOpResult> results = new ConcurrentHashMap<>(files.size());
      var stage = CompletionStages.aggregateCompletionStage(results);
      files.forEach((schemaName, schemaContent) -> {
         var f = schemaOp.apply(Schema.buildFromStringContent(schemaName, schemaContent))
               .thenAccept(schemaOpResult -> results.put(schemaName, schemaOpResult));
         stage.dependsOn(f);
      });
      return stage.freeze().toCompletableFuture();
   }

   @Override
   public CompletionStage<SchemaOpResult> removeAsync(String name, boolean force) {
      if (force) {
         return protostreamCache.withFlags(FORCE_RETURN_VALUE)
               .removeAsync(name).thenApply(r -> {
                  if (r == null) {
                     log.schemaNotFound(name);
                     return new SchemaOpResult(SchemaOpResultType.NONE);
                  }
                  return new SchemaOpResult(SchemaOpResultType.DELETED);
               });
      }

      Map<String, byte[]> params = Map.of("name", string(name));
      return operationDispatcher
            .execute(operationsFactory.executeOperation("@@schemas@delete", params))
            .thenApply(r -> new SchemaOpResult(getSchemaOpResult(name, r)));
   }

   @Override
   public CompletionStage<SchemaErrors> retrieveAllSchemaErrorsAsync() {
      return CompletionStages.handleAndCompose(
            protostreamCache.getAsync(SchemaErrors.ERRORS_KEY_SUFFIX),
            (errors, ex) -> {
               if (ex != null) {
                  log.schemasInErrorRetrieveFailure(ex);
                  throw new InfinispanException(ex);
               } else if (errors != null) {
                  Set<String> errorFileKeys = Stream.of(errors.split("\n"))
                        .map(errorFile -> errorFile + SchemaErrors.ERRORS_KEY_SUFFIX)
                        .collect(Collectors.toSet());

                  return protostreamCache.getAllAsync(errorFileKeys)
                        .thenApply(SchemaErrors::new)
                        .exceptionally(ex2 -> {
                           log.schemasInErrorRetrieveFailure(errorFileKeys.toString(), ex2);
                           throw new InfinispanException(ex2);
                        });
               }
               return CompletableFuture.completedFuture(new SchemaErrors(null));
            });
   }

   @Override
   public CompletionStage<Optional<String>> retrieveErrorAsync(String schemaName) {
      if (log.isDebugEnabled()) {
         return CompletionStages.handleAndCompose(protostreamCache.getAsync(schemaName), (schemaContent, ex) -> {
            if (ex != null) {
               log.crudSchemaError(schemaName, ex);
               throw new InfinispanException(ex);
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
            .thenApply(RemoteSchemasAdminImpl::createOrUpdateToSchemaOpResult)
            .exceptionally(ex -> {
               log.crudSchemaError(schema.getName(), ex);
               throw new InfinispanException(ex);
            });
   }

    private static Optional<Schema> getSchemaContent(String schemaName, String schemaContent) {
        if (schemaContent != null) {
            return Optional.of(Schema.buildFromStringContent(schemaName, schemaContent));
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
            log.notSafeDelete(schemaName);
            throw new InfinispanException(String.format("A cache is using one entity in the schema %s. Remove not done", schemaName));
        }

        if (safeDelete == 1) {
            return SchemaOpResultType.DELETED;
        }

        log.debugf(String.format("Schema %s not found", schemaName));
        return SchemaOpResultType.NONE;
    }

    private CompletionStage<Optional<String>> getErrorAsync(String schemaName) {
       return protostreamCache.getAsync(schemaName + SchemaErrors.ERRORS_KEY_SUFFIX)
             .thenApply(Optional::ofNullable)
             .exceptionally(ex -> {
                log.crudSchemaError(schemaName, ex);
                throw new InfinispanException(ex);
             });
    }
}
