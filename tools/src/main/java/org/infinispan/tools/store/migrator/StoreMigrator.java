package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.BATCH;
import static org.infinispan.tools.store.migrator.Element.SIZE;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.util.KeyValuePair;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public class StoreMigrator {

   private static final int DEFAULT_BATCH_SIZE = 1;
   // A list of all internal classes that were previously marshallable, but we no longer provide
   // guarantees over byte compatibility in future versions
   private static final Set<Class<?>> INTERNAL_BLACKLIST = new HashSet<>();
   static {
      INTERNAL_BLACKLIST.add(ArrayList.class);
      INTERNAL_BLACKLIST.add(Collections.singletonList(1).getClass());
      INTERNAL_BLACKLIST.add(Collections.singletonMap(1,1).getClass());
      INTERNAL_BLACKLIST.add(Collections.singleton(1).getClass());
      INTERNAL_BLACKLIST.add(ByteBufferImpl.class);
      INTERNAL_BLACKLIST.add(KeyValuePair.class);
      INTERNAL_BLACKLIST.add(InternalCacheEntry.class);
      INTERNAL_BLACKLIST.add(InternalCacheValue.class);
      INTERNAL_BLACKLIST.add(InternalMetadataImpl.class);
      INTERNAL_BLACKLIST.add(ImmortalCacheEntry.class);
      INTERNAL_BLACKLIST.add(MortalCacheEntry.class);
      INTERNAL_BLACKLIST.add(TransientCacheEntry.class);
      INTERNAL_BLACKLIST.add(TransientMortalCacheEntry.class);
      INTERNAL_BLACKLIST.add(ImmortalCacheValue.class);
      INTERNAL_BLACKLIST.add(MortalCacheValue.class);
      INTERNAL_BLACKLIST.add(TransientCacheValue.class);
      INTERNAL_BLACKLIST.add(TransientMortalCacheValue.class);

      INTERNAL_BLACKLIST.add(MetadataImmortalCacheEntry.class);
      INTERNAL_BLACKLIST.add(MetadataMortalCacheEntry.class);
      INTERNAL_BLACKLIST.add(MetadataTransientCacheEntry.class);
      INTERNAL_BLACKLIST.add(MetadataTransientMortalCacheEntry.class);
      INTERNAL_BLACKLIST.add(MetadataImmortalCacheValue.class);
      INTERNAL_BLACKLIST.add(MetadataMortalCacheValue.class);
      INTERNAL_BLACKLIST.add(MetadataTransientCacheValue.class);
      INTERNAL_BLACKLIST.add(MetadataTransientMortalCacheValue.class);
   }
   private final Properties properties;

   public StoreMigrator(Properties properties) {
      this.properties = properties;
   }

   public void run() throws Exception {
      run(false);
   }

   void run(boolean output) throws Exception {
      String batchSizeProp = properties.getProperty(BATCH + "." + SIZE);
      int batchLimit = batchSizeProp != null ? new Integer(batchSizeProp) : DEFAULT_BATCH_SIZE;

      try (EmbeddedCacheManager manager = TargetStoreFactory.getCacheManager(properties);
           StoreIterator sourceReader = StoreIteratorFactory.get(properties)) {

         Map<Integer, AdvancedExternalizer<?>> externalizers = manager.getCacheManagerConfiguration().serialization().advancedExternalizers();
         Set<Class> externalizerClasses = externalizers.values().stream()
               .flatMap(e -> e.getTypeClasses().stream())
               .collect(Collectors.toSet());

         AdvancedCache targetCache = TargetStoreFactory.getTargetCache(manager, properties);
         // Txs used so that writes to the DB are batched. Migrator will always operate locally Tx overhead should be negligible
         TransactionManager tm = targetCache.getTransactionManager();
         int txBatchSize = 0;
         for (MarshallableEntry entry : sourceReader) {
            if (warnAndIgnoreInternalClasses(entry.getKey(), externalizerClasses, output) ||
                  warnAndIgnoreInternalClasses(entry.getValue(), externalizerClasses, output))
               continue;

            if (txBatchSize == 0)
               tm.begin();

            targetCache.put(entry.getKey(), entry.getValue());
            txBatchSize++;

            if (txBatchSize == batchLimit) {
               txBatchSize = 0;
               tm.commit();
            }
         }
         if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
      }
   }

   public static void main(String[] args) throws Exception {
      if (args.length != 1) {
         System.err.println("Usage: StoreMigrator migrator.properties");
         System.exit(1);
      }
      Properties properties = new Properties();
      properties.load(new FileReader(args[0]));
      new StoreMigrator(properties).run(true);
   }

   private boolean warnAndIgnoreInternalClasses(Object o, Set<Class> extClass, boolean output) {
      Class clazz = o.getClass();
      boolean isBlackListed = !extClass.contains(clazz) && !clazz.isPrimitive() && INTERNAL_BLACKLIST.stream().anyMatch(c -> c.isAssignableFrom(clazz));
      if (isBlackListed) {
         if (output) {
            System.err.println(String.format("Ignoring entry with class %s as this is an internal Infinispan class that" +
                  "should not be used by users. If you really require this class, it's possible to explicitly provide the" +
                  "associated AdvancedExternalizer via the property 'target.marshaller.externalizers=Externalizer.class`", o.getClass()));
         }
         return true;
      }
      return false;
   }
}
