package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.BATCH;
import static org.infinispan.tools.store.migrator.Element.SIZE;

import java.io.FileReader;
import java.util.Properties;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.MarshallableEntry;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public class StoreMigrator {

   private static final int DEFAULT_BATCH_SIZE = 1;
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
         for (MarshallableEntry entry : sourceReader) {
            if (txBatchSize == 0) tm.begin();

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
}
