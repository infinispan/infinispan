package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.BATCH;
import static org.infinispan.tools.store.migrator.Element.SIZE;

import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.util.Version;
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

import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;

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

   public void run(boolean output) throws Exception {
      String batchSizeProp = properties.getProperty(BATCH + "." + SIZE);
      int batchLimit = batchSizeProp != null ? Integer.parseInt(batchSizeProp) : DEFAULT_BATCH_SIZE;

      try (EmbeddedCacheManager manager = TargetStoreFactory.getCacheManager(properties);
           StoreIterator sourceReader = StoreIteratorFactory.get(properties)) {

         AdvancedCache targetCache = TargetStoreFactory.getTargetCache(manager, properties);
         // Txs used so that writes to the DB are batched. Migrator will always operate locally Tx overhead should be negligible
         TransactionManager tm = targetCache.getTransactionManager();
         int txBatchSize = 0;
         for (MarshallableEntry entry : sourceReader) {
            if (warnAndIgnoreInternalClasses(entry.getKey(), output) || warnAndIgnoreInternalClasses(entry.getValue(), output))
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
         version(System.out);
         System.out.println("Usage: StoreMigrator migrator.properties");
         System.exit(1);
      }
      Properties properties = new Properties();
      properties.load(new FileReader(args[0]));
      new StoreMigrator(properties).run(true);
   }

   private static void version(PrintStream out) {
      out.printf("%s Store Migrator %s%n", Version.getBrandName(), Version.getBrandVersion());
      out.println("Copyright (C) Red Hat Inc. and/or its affiliates and other contributors");
      out.println("License Apache License, v. 2.0. http://www.apache.org/licenses/LICENSE-2.0");
   }

   private boolean warnAndIgnoreInternalClasses(Object o, boolean output) {
      Class<?> clazz = o.getClass();
      boolean isBlackListed = !clazz.isPrimitive() && INTERNAL_BLACKLIST.stream().anyMatch(c -> c.isAssignableFrom(clazz));
      if (isBlackListed) {
         if (output) {
            System.err.printf("Ignoring entry with class '%s' as this is an internal Infinispan class that" +
                  "should not be used by users. If you really require this class, it's possible to explicitly provide the" +
                  "Infinispan SerializationContextInitializers via the property 'target.marshaller.context-initializers=%n", o.getClass());
         }
         return true;
      }
      return false;
   }
}
