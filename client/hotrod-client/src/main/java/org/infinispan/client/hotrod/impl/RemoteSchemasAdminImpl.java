package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.operations.ManagerOperationsFactory;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.admin.SchemaAdminRuntimeException;
import org.infinispan.commons.admin.SchemasAdministration;
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

public class RemoteSchemasAdminImpl implements SchemasAdministration {
   public static final Log log = LogFactory.getLog(RemoteSchemasAdminImpl.class, Log.class);

   private final ManagerOperationsFactory operationsFactory;
   private final OperationDispatcher operationDispatcher;
   private final RemoteCache<String, String> protostreamCache;

   protected RemoteSchemasAdminImpl(ManagerOperationsFactory operationsFactory, OperationDispatcher operationDispatcher, RemoteCacheManager cacheManager) {
       this.operationsFactory = operationsFactory;
       this.operationDispatcher = operationDispatcher;
       this.protostreamCache = cacheManager.<String, String>getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME).withFlags(FORCE_RETURN_VALUE);
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
     return result == null ? SchemaOpResult.created() : SchemaOpResult.none();
   }

   @Override
   public CompletionStage<SchemaOpResult> createAsync(Schema schema) {
     return protostreamCache.putIfAbsentAsync(schema.getName(), schema.getContent())
             .thenApply(r -> r == null ? SchemaOpResult.created() : SchemaOpResult.none());
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
         return prevValue != null ? SchemaOpResult.updated() : SchemaOpResult.none();
      }

      var stored = protostreamCache.getWithMetadata(schema.getName());
      boolean updateResult = false;
      if (stored != null && !Objects.equals(stored.getValue(), schema.getContent())) {
         if (protostreamCache.replaceWithVersion(schema.getName(), schema.getContent(), stored.getVersion())) {
            updateResult = true;
         }
      }
      return updateResult ? SchemaOpResult.updated() : SchemaOpResult.none();
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
                 .thenApply(prevValue -> prevValue != null ? SchemaOpResult.updated() : SchemaOpResult.none())
                 .exceptionally(ex -> {
                    if (ex != null) {
                       log.error(ex);
                       throw new SchemaAdminRuntimeException(ex);
                    }
                    return SchemaOpResult.none();
                 });
      }

      return CompletionStages.handleAndCompose(protostreamCache.getWithMetadataAsync(schema.getName()),
              (stored, ex) -> {
                 if (ex != null) {
                    log.error(ex);
                    throw new SchemaAdminRuntimeException(ex);
                 }

                 if (stored == null) {
                    // If it does not exist, there is  nothing to be updated
                    return CompletableFuture.completedFuture(SchemaOpResult.none());
                 }

                 if (stored != null && !Objects.equals(stored.getValue(), schema.getContent())) {
                    return protostreamCache.replaceWithVersionAsync(schema.getName(), schema.getContent(), stored.getVersion())
                            .thenApply(r -> r ? SchemaOpResult.updated() : SchemaOpResult.none())
                            .exceptionally(ex2 -> {
                               if (ex2 != null) {
                                  throw new SchemaAdminRuntimeException(ex2);
                               }
                               return SchemaOpResult.none();
                            });
                 }
                 // Nothing has been done
                 return CompletableFuture.completedFuture(SchemaOpResult.none());
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
         return prev == null ? SchemaOpResult.created() : SchemaOpResult.updated();
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
                 .thenApply(r -> r == null ? SchemaOpResult.created(): SchemaOpResult.updated())
                 .exceptionally(ex -> {
                    if (ex != null) {
                       throw new SchemaAdminRuntimeException(ex);
                    }
                    return SchemaOpResult.none();
                 });
      }

      return CompletionStages.handleAndCompose(protostreamCache.getWithMetadataAsync(schema.getName()), (actualSchema, ex) -> {
         if (ex != null) {
            throw new SchemaAdminRuntimeException(ex);
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
                    throw new SchemaAdminRuntimeException(ex);
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
      String schema = protostreamCache.get(name);
      if (schema == null) {
         String deleteMessage = String.format("Schema %s not found", name);
         log.debugf(deleteMessage);
         return SchemaOpResult.none();
      }

      if (force) {
         protostreamCache.remove(name);
         return SchemaOpResult.deleted();
      }

      // Check that the schema types are not referenced by any indexed cache
      Map<String, byte[]> params = new HashMap<>(4);
      params.put("schema", string(name));
      boolean safeDelete = Boolean.parseBoolean(await(operationDispatcher.execute(operationsFactory.executeOperation("@@schemas@validate", params))));
      if (safeDelete) {
         protostreamCache.remove(name);
         return SchemaOpResult.deleted();
      }

      throw new SchemaAdminRuntimeException(String.format("A cache is using one entity in the schema %s. Delete not done", name));
   }

   @Override
   public CompletionStage<SchemaOpResult> removeAsync(String name) {
      return removeAsync(name, false);
   }

   @Override
   public CompletionStage<SchemaOpResult> removeAsync(String name, boolean force) {
      return CompletionStages.handleAndCompose(protostreamCache.getAsync(name), (schema, ex) -> {
         if (ex != null) {
           throw new SchemaAdminRuntimeException(ex);
         }
         if (schema == null) {
            String deleteMessage = String.format("Schema %s not found", name);
            log.debugf(deleteMessage);
            return CompletableFuture.completedFuture(SchemaOpResult.none());
         }

         if (force) {
            return protostreamCache.removeAsync(name).thenApply(r -> SchemaOpResult.deleted());
         }

         // Check that the schema types are not referenced by any indexed cache
         Map<String, byte[]> params = new HashMap<>(4);
         params.put("schema", string(name));

         return CompletionStages.handleAndCompose(operationDispatcher
                 .execute(operationsFactory.executeOperation("@@schemas@validate", params)), (opResult, ex2) -> {
            if (ex2 != null) {
               throw new SchemaAdminRuntimeException(ex2);
            }

            boolean safeDelete = Boolean.parseBoolean(opResult);
            if (safeDelete) {
               return protostreamCache.removeAsync(name)
                       .thenApply(r -> SchemaOpResult.deleted())
                       .exceptionally(ex3 -> {
                          if (ex3 != null) {
                             throw new SchemaAdminRuntimeException(ex);
                          }
                          return SchemaOpResult.none();
                       });
            }
            throw new SchemaAdminRuntimeException(String.format("A cache is using one entity in the schema %s. Delete not done", name));
         });
      });
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
              .map(errorFile-> errorFile + SchemaErrors.ERRORS_KEY_SUFFIX)
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
                    .map(errorFile-> errorFile + SchemaErrors.ERRORS_KEY_SUFFIX)
                    .collect(Collectors.toSet());

            return protostreamCache.getAllAsync(errorFileKeys)
                    .thenApply((mapErrors) -> {
                        schemaErrors.addAll(mapErrors);
                        return schemaErrors;
                    }).exceptionally(ex2 -> {
                       if (ex2 != null) {
                          log.error(ex2.getMessage(), ex2);
                       }
                       return schemaErrors;
                    });
         }
         return CompletableFuture.completedFuture(schemaErrors);
      });
   }

   @Override
   public boolean exists(String schemaName) {
      return protostreamCache.get(schemaName) != null;
   }

   @Override
   public Optional<String> retrieveError(String schemaName) {
      String schema = protostreamCache.get(schemaName);
      if (schema == null) {
         log.debugf(String.format("Schema %s not found", schemaName));
         return Optional.empty();
      }

      String error = protostreamCache.get(schemaName + SchemaErrors.ERRORS_KEY_SUFFIX);
      return Optional.ofNullable(error);
   }

   @Override
   public CompletionStage<Optional<String>> retrieveErrorAsync(String schemaName) {
      return CompletionStages.handleAndCompose(protostreamCache.getAsync(schemaName), (schema, ex) -> {
         if (ex != null) {
            throw new SchemaAdminRuntimeException(ex);
         }

         if (schema == null) {
            log.debugf(String.format("Schema %s not found", schemaName));
            return CompletableFuture.completedFuture(Optional.empty());
         }

         return protostreamCache.getAsync(schemaName + SchemaErrors.ERRORS_KEY_SUFFIX)
                 .thenApply(error -> Optional.ofNullable(error))
                 .exceptionally(ex2 -> {
                    if (ex2 != null) {
                       throw new SchemaAdminRuntimeException(ex2);
                    }
                    return Optional.empty();
                 });
      });
   }
}
