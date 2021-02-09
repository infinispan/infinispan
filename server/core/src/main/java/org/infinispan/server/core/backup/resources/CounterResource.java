package org.infinispan.server.core.backup.resources;

import static org.infinispan.server.core.BackupManager.Resources.Type.COUNTERS;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.core.BackupManager;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;

import io.reactivex.rxjava3.core.Flowable;

/**
 * {@link org.infinispan.server.core.backup.ContainerResource} implementation for {@link
 * BackupManager.Resources.Type#COUNTERS}.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
public class CounterResource extends AbstractContainerResource {

   private static final String COUNTERS_FILE = "counters.dat";

   private final CounterManager counterManager;
   private final ImmutableSerializationContext serCtx;

   CounterResource(BlockingManager blockingManager, EmbeddedCacheManager cm,
                   BackupManager.Resources params, Path root) {
      super(COUNTERS, params, blockingManager, root);
      GlobalComponentRegistry gcr = cm.getGlobalComponentRegistry();
      this.counterManager = gcr.getComponent(CounterManager.class);
      this.serCtx = gcr.getComponent(SerializationContextRegistry.class).getPersistenceCtx();
   }

   @Override
   public void prepareAndValidateBackup() {
      if (wildcard) {
         resources.addAll(counterManager.getCounterNames());
         return;
      }

      for (String counterName : resources) {
         if (counterManager.getConfiguration(counterName) == null)
            throw log.unableToFindResource(type.toString(), counterName);
      }
   }

   @Override
   public CompletionStage<Void> backup() {
      return blockingManager.blockingPublisherToVoidStage(
            Flowable.using(
                  () -> {
                     mkdirs(root);
                     return Files.newOutputStream(root.resolve(COUNTERS_FILE));
                  },
                  output ->
                        Flowable.fromIterable(resources)
                              .map(counter -> {
                                 CounterConfiguration config = counterManager.getConfiguration(counter);
                                 CounterBackupEntry e = new CounterBackupEntry();
                                 e.name = counter;
                                 e.configuration = config;
                                 e.value = config.type() == CounterType.WEAK ?
                                       counterManager.getWeakCounter(counter).getValue() :
                                       CompletionStages.join(counterManager.getStrongCounter(counter).getValue());
                                 return e;
                              })
                              .doOnNext(e -> {
                                 writeMessageStream(e, serCtx, output);
                                 log.debugf("Counter added to backup: %s", e);
                              })
                              .onErrorResumeNext(RxJavaInterop.cacheExceptionWrapper()),
                  OutputStream::close
            ), "write-counters");
   }

   @Override
   public CompletionStage<Void> restore(ZipFile zip) {
      return blockingManager.runBlocking(() -> {
         Set<String> countersToRestore = resources;
         String countersFile = root.resolve(COUNTERS_FILE).toString();
         ZipEntry zipEntry = zip.getEntry(countersFile);
         if (zipEntry == null) {
            if (!countersToRestore.isEmpty())
               throw log.unableToFindBackupResource(type.toString(), countersToRestore);
            return;
         }

         try (InputStream is = zip.getInputStream(zipEntry)) {
            while (is.available() > 0) {
               CounterBackupEntry entry = readMessageStream(serCtx, CounterBackupEntry.class, is);
               if (!countersToRestore.contains(entry.name)) {
                  log.debugf("Ignoring '%s' counter", entry.name);
                  continue;
               }
               CounterConfiguration config = entry.configuration;
               counterManager.defineCounter(entry.name, config);
               if (config.type() == CounterType.WEAK) {
                  WeakCounter counter = counterManager.getWeakCounter(entry.name);
                  counter.add(entry.value - config.initialValue());
               } else {
                  StrongCounter counter = counterManager.getStrongCounter(entry.name);
                  counter.compareAndSet(config.initialValue(), entry.value);
               }
               log.debugf("Counter restored: %s", entry);
            }
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }, "restore-counters");
   }

   /**
    * ProtoStream entity used to represent counter instances.
    */
   @ProtoTypeId(ProtoStreamTypeIds.COUNTER_BACKUP_ENTRY)
   public static class CounterBackupEntry {

      @ProtoField(number = 1)
      String name;

      @ProtoField(number = 2)
      CounterConfiguration configuration;

      @ProtoField(number = 3, defaultValue = "-1")
      long value;

      @Override
      public String toString() {
         return "CounterBackupEntry{" +
               "name='" + name + '\'' +
               ", configuration=" + configuration +
               ", value=" + value +
               '}';
      }
   }
}
