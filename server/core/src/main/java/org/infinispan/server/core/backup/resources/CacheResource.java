package org.infinispan.server.core.backup.resources;

import static org.infinispan.globalstate.impl.GlobalConfigurationManagerImpl.CACHE_SCOPE;
import static org.infinispan.server.core.BackupManager.Resources.Type.CACHES;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.globalstate.impl.CacheState;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.reactive.publisher.PublisherTransformers;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.core.BackupManager;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.function.SerializableFunction;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * {@link org.infinispan.server.core.backup.ContainerResource} implementation for {@link
 * BackupManager.Resources.Type#CACHES}.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
public class CacheResource extends AbstractContainerResource {

   private static final String MEMCACHED_CACHE = "memcachedCache";

   private final EmbeddedCacheManager cm;
   private final ParserRegistry parserRegistry;

   CacheResource(BlockingManager blockingManager, ParserRegistry parserRegistry, EmbeddedCacheManager cm,
                 BackupManager.Resources params, Path root) {
      super(CACHES, params, blockingManager, root);
      this.cm = cm;
      this.parserRegistry = parserRegistry;
   }

   @Override
   public void prepareAndValidateBackup() {
      InternalCacheRegistry icr = SecurityActions.getGlobalComponentRegistry(cm).getComponent(InternalCacheRegistry.class);

      Set<String> caches = wildcard ? cm.getCacheConfigurationNames() : resources;
      for (String cache : caches) {
         Configuration config = SecurityActions.getCacheConfiguration(cm, cache);

         if (wildcard) {
            // For wildcard resources, we ignore internal caches, however explicitly requested internal caches are allowed
            if (config == null || config.isTemplate() || icr.isInternalCache(cache) || MEMCACHED_CACHE.equals(cache)) {
               continue;
            }
            resources.add(cache);
         } else if (config == null) {
            throw log.unableToFindResource(type.toString(), cache);
         } else if (config.isTemplate()) {
            throw new CacheException(String.format("Unable to backup %s '%s' as it is a template not a cache", type, cache));
         }
      }
   }

   @Override
   public CompletionStage<Void> backup() {
      AggregateCompletionStage<Void> stages = CompletionStages.aggregateCompletionStage();
      for (String cache : resources)
         stages.dependsOn(createCacheBackup(cache));
      return stages.freeze();
   }

   @Override
   public CompletionStage<Void> restore(ZipFile zip) {
      AggregateCompletionStage<Void> stages = CompletionStages.aggregateCompletionStage();

      for (String cacheName : resources) {
         stages.dependsOn(blockingManager.runBlocking(() -> {
            Path cacheRoot = root.resolve(cacheName);

            // Process .xml
            String configFile = configFile(cacheName);
            String zipPath = cacheRoot.resolve(configFile).toString();
            try (InputStream is = zip.getInputStream(zip.getEntry(zipPath))) {
               ConfigurationBuilderHolder builderHolder = parserRegistry.parse(is, null, MediaType.fromExtension(configFile));
               Configuration config = builderHolder.getNamedConfigurationBuilders().get(cacheName).build();
               log.debugf("Restoring Cache %s: %s", cacheName, config.toXMLString(cacheName));

               String configXml = config.toXMLString(cacheName);
               SerializableFunction<EmbeddedCacheManager, Void> createCacheFunction = m -> {
                  GlobalConfiguration globalConfig = SecurityActions.getGlobalConfiguration(m);

                  log.debugf("Create cache %s locally. config=%s", cacheName, configXml);
                  ConfigurationBuilderHolder cbh = new ParserRegistry().parse(configXml);
                  Configuration configuration = cbh.getNamedConfigurationBuilders().get(cacheName).build(globalConfig);
                  if (!m.getCacheConfigurationNames().contains(cacheName)) {
                     m.defineConfiguration(cacheName, configuration);
                  }
                  m.getCache(cacheName);
                  return null;
               };
               ClusterExecutor executor = SecurityActions.getClusterExecutor(cm);
               CompletableFuture<Void> remoteCall = executor.submitConsumer(createCacheFunction, (a, v, t) -> {
                  if (t != null) {
                     throw new CacheException(t);
                  }
               });

               // Insert the cache configuration in the CONFIG state in parallel
               // Note: cm.admin().getOrCreateCache() looks better, but it is not guaranteed to insert the entry
               // in the CONFIG cache if the cache already exists locally.
               log.debugf("Define cache %s globally. config=%s", cacheName, configXml);
               CacheState state = new CacheState(null, configXml, EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class));
               GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(cm);
               gcr.getComponent(GlobalConfigurationManager.class).getStateCache().getAdvancedCache()
                  .withFlags(Flag.IGNORE_RETURN_VALUES)
                  .putIfAbsent(new ScopedState(CACHE_SCOPE, cacheName), state);

               // Wait for the cache to be started everywhere
               CompletionStages.join(remoteCall);
            } catch (IOException e) {
               throw new CacheException(e);
            }

            // Process .dat
            String dataFile = dataFile(cacheName);
            String data = cacheRoot.resolve(dataFile).toString();
            ZipEntry zipEntry = zip.getEntry(data);
            if (zipEntry == null)
               return;

            AdvancedCache<Object, Object> cache = cm.getCache(cacheName).getAdvancedCache();
            ComponentRegistry cr = SecurityActions.getComponentRegistry(cache);
            CommandsFactory commandsFactory = cr.getCommandsFactory();
            KeyPartitioner keyPartitioner = cr.getComponent(KeyPartitioner.class);
            InvocationHelper invocationHelper = cr.getComponent(InvocationHelper.class);
            StorageConfigurationManager scm = cr.getComponent(StorageConfigurationManager.class);
            PersistenceMarshaller persistenceMarshaller = cr.getPersistenceMarshaller();
            Marshaller userMarshaller = persistenceMarshaller.getUserMarshaller();

            boolean keyMarshalling = !scm.getKeyStorageMediaType().isBinary();
            boolean valueMarshalling = !scm.getValueStorageMediaType().isBinary();

            SerializationContextRegistry ctxRegistry = SecurityActions.getGlobalComponentRegistry(cm).getComponent(SerializationContextRegistry.class);
            ImmutableSerializationContext serCtx = ctxRegistry.getPersistenceCtx();

            int entries = 0;
            try (DataInputStream is = new DataInputStream(zip.getInputStream(zipEntry))) {
               while (is.available() > 0) {
                  CacheBackupEntry entry = readMessageStream(serCtx, CacheBackupEntry.class, is);
                  Object key = keyMarshalling ? unmarshall(entry.key, userMarshaller) : scm.getKeyWrapper().wrap(entry.key);
                  Object value = valueMarshalling ? unmarshall(entry.value, userMarshaller) : scm.getValueWrapper().wrap(entry.value);
                  Metadata metadata = unmarshall(entry.metadata, persistenceMarshaller);
                  Metadata internalMetadataImpl = new InternalMetadataImpl(metadata, entry.created, entry.lastUsed);

                  PutKeyValueCommand cmd = commandsFactory.buildPutKeyValueCommand(key, value, keyPartitioner.getSegment(key),
                        internalMetadataImpl, FlagBitSets.IGNORE_RETURN_VALUES);
                  cmd.setInternalMetadata(entry.internalMetadata);
                  invocationHelper.invoke(cmd, 1);
                  entries++;
               }
            } catch (IOException e) {
               throw new CacheException(e);
            }
            log.debugf("Cache %s restored %d entries", cacheName, entries);
         }, "restore-cache-" + cacheName));
      }
      return stages.freeze();
   }

   private CompletionStage<Void> createCacheBackup(String cacheName) {
      return blockingManager.runBlocking(() -> {
         AdvancedCache<?, ?> cache = cm.getCache(cacheName).getAdvancedCache();
         Configuration configuration = SecurityActions.getCacheConfiguration(cm, cacheName);

         Path cacheRoot = root.resolve(cacheName);
         // Create the cache backup dir and parents
         mkdirs(cacheRoot);

         // Write configuration file
         String xmlFileName = configFile(cacheName);
         Path xmlPath = cacheRoot.resolve(xmlFileName);
         try (OutputStream os = Files.newOutputStream(xmlPath)) {
            parserRegistry.serialize(os, cacheName, configuration);
         } catch (IOException e) {
            throw new CacheException(String.format("Unable to create backup file '%s'", xmlFileName), e);
         }

         ComponentRegistry cr = SecurityActions.getComponentRegistry(cache);
         ClusterPublisherManager<Object, Object> clusterPublisherManager = cr.getClusterPublisherManager().running();
         SerializationContextRegistry ctxRegistry = cr.getGlobalComponentRegistry().getComponent(SerializationContextRegistry.class);
         ImmutableSerializationContext serCtx = ctxRegistry.getPersistenceCtx();

         String dataFileName = dataFile(cacheName);
         Path datFile = cacheRoot.resolve(dataFileName);

         int bufferSize = configuration.clustering().stateTransfer().chunkSize();

         // Create the publisher using the BlockingManager to ensure that all entries are subscribed to on a blocking thread
         Publisher<CacheEntry<Object, Object>> p = blockingManager.blockingPublisher(
               clusterPublisherManager.entryPublisher(null, null, null, true,
                     DeliveryGuarantee.EXACTLY_ONCE, bufferSize, PublisherTransformers.identity())
         );

         StorageConfigurationManager scm = cr.getComponent(StorageConfigurationManager.class);
         boolean keyMarshalling = !scm.getKeyStorageMediaType().isBinary();
         boolean valueMarshalling = !scm.getValueStorageMediaType().isBinary();
         PersistenceMarshaller persistenceMarshaller = cr.getPersistenceMarshaller();
         Marshaller userMarshaller = persistenceMarshaller.getUserMarshaller();

         log.debugf("Backing up Cache %s", configuration.toXMLString(cacheName));
         final AtomicInteger entries = new AtomicInteger();
         Flowable.using(
               () -> new DataOutputStream(Files.newOutputStream(datFile)),
               output ->
                     Flowable.fromPublisher(p)
                           .map(e -> {
                              CacheBackupEntry be = new CacheBackupEntry();
                              be.key = keyMarshalling ? marshall(e.getKey(), userMarshaller) : (byte[]) scm.getKeyWrapper().unwrap(e.getKey());
                              be.value = valueMarshalling ? marshall(e.getValue(), userMarshaller) : (byte[]) scm.getValueWrapper().unwrap(e.getValue());
                              be.metadata = marshall(e.getMetadata(), persistenceMarshaller);
                              be.internalMetadata = e.getInternalMetadata();
                              be.created = e.getCreated();
                              be.lastUsed = e.getLastUsed();
                              return be;
                           })
                           .doOnNext(e -> {
                              entries.incrementAndGet();
                              writeMessageStream(e, serCtx, output);
                           })
                           .onErrorResumeNext(RxJavaInterop.cacheExceptionWrapper())
                           .doOnComplete(() -> log.debugf("Cache %s backed up %d entries", cacheName, entries.get())),
               OutputStream::close)
               .subscribe();
      }, "backup-cache");
   }

   private String configFile(String cache) {
      return String.format("%s.xml", cache);
   }

   private String dataFile(String cache) {
      return String.format("%s.dat", cache);
   }

   private byte[] marshall(Object key, Marshaller marshaller) {
      try {
         return marshaller.objectToByteBuffer(key);
      } catch (IOException | InterruptedException e) {
         throw new MarshallingException(e);
      }
   }

   @SuppressWarnings("unchecked")
   private static <T> T unmarshall(byte[] bytes, Marshaller marshaller) {
      try {
         return (T) marshaller.objectFromByteBuffer(bytes);
      } catch (ClassNotFoundException | IOException e) {
         throw new MarshallingException(e);
      }
   }

   /**
    * ProtoStream entity used to represent individual cache entries.
    */
   @ProtoTypeId(ProtoStreamTypeIds.CACHE_BACKUP_ENTRY)
   public static class CacheBackupEntry {

      @ProtoField(number = 1)
      byte[] key;

      @ProtoField(number = 2)
      byte[] value;

      @ProtoField(number = 3)
      byte[] metadata;

      @ProtoField(number = 4)
      PrivateMetadata internalMetadata;

      @ProtoField(number = 5, defaultValue = "-1")
      long created;

      @ProtoField(number = 6, defaultValue = "-1")
      long lastUsed;
   }
}
