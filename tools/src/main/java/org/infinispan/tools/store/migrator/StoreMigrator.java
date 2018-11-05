package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.BATCH;
import static org.infinispan.tools.store.migrator.Element.SIZE;

import java.io.FileReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.MarshalledEntry;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public class StoreMigrator {

   private static final int DEFAULT_BATCH_SIZE = 1;
   private static final Set<Class<?>> IMPL_BLACKLIST = new HashSet<>();
   static {
      IMPL_BLACKLIST.add(InternalCacheEntry.class);
      IMPL_BLACKLIST.add(InternalCacheValue.class);
   }
   private final Properties properties;

   public StoreMigrator(Properties properties) {
      this.properties = properties;
   }

   public void run() throws Exception {
      String batchSizeProp = properties.getProperty(BATCH + "." + SIZE);
      int batchLimit = batchSizeProp != null ? new Integer(batchSizeProp) : DEFAULT_BATCH_SIZE;

      try (EmbeddedCacheManager manager = TargetStoreFactory.getCacheManager(properties);
           StoreIterator sourceReader = StoreIteratorFactory.get(properties)) {

         AdvancedCache targetCache = TargetStoreFactory.getTargetCache(manager, properties);
         // Txs used so that writes to the DB are batched. Migrator will always operate locally Tx overhead should be negligible
         TransactionManager tm = targetCache.getTransactionManager();
         int txBatchSize = 0;
         for (MarshalledEntry entry : sourceReader) {
            if (warnAndIgnoreInternalClasses(entry.getKey()) || warnAndIgnoreInternalClasses(entry.getValue()))
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
      new StoreMigrator(properties).run();
   }

   private boolean warnAndIgnoreInternalClasses(Object o) {
      Class clazz = o.getClass();
      boolean isBlackListed = !clazz.isPrimitive() && IMPL_BLACKLIST.stream().anyMatch(c -> c.isAssignableFrom(clazz));
      if (isBlackListed) {
         // TODO enable custom protostream marshallers to be added so that uses can continue to use internal classes if they really want
         System.err.println(String.format("Ignoring entry with class %s as this is an internal Infinispan class that should not be used by users", o.getClass()));
         return true;
      }
      return false;
   }
}
