package org.infinispan.persistence.jpa;

import static org.testng.Assert.assertEquals;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.jpa.entity.Huge;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

/**
 * Checks that even lot of huge entries don't cause OOME
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "stress", testName = "persistence.JpaStoreOOMETest")
public class JpaStoreOOMETest extends AbstractJpaStoreTest {

   private final static int NUM_ENTRIES = 1 << 14;
   private final static int VALUE_SIZE = 1 << 16;
   private final static long LIFESPAN = 1000;

   public void testProcessClear() {
      assertEquals(cs.size(), 0);
      fillStore(NUM_ENTRIES, VALUE_SIZE, null);
      assertEquals(cs.size(), NUM_ENTRIES);
      final AtomicInteger processed = new AtomicInteger();
      cs.process(null, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            int entries = processed.incrementAndGet();
            if (entries % 100 == 0) {
               log.info("Processed " + entries + " entries");
            }
         }
      }, new WithinThreadExecutor(), true, false);
      assertEquals(processed.get(), NUM_ENTRIES);
      cs.clear();
      assertEquals(cs.size(), 0);
   }

   public void testPurge() {
      assertEquals(cs.size(), 0);
      fillStore(NUM_ENTRIES, VALUE_SIZE, new LifespanProvider() {
         private boolean even = false;
         @Override
         public long get() {
            even = !even;
            return even ? LIFESPAN : -1;
         }
      });
      try {
         // make sure all entries have expired lifespan
         Thread.sleep(LIFESPAN);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      final AtomicInteger purged = new AtomicInteger();
      cs.purge(new WithinThreadExecutor(), new AdvancedCacheWriter.PurgeListener() {
         @Override
         public void entryPurged(Object key) {
            int entries = purged.incrementAndGet();
            if (entries % 100 == 0) {
               log.info("Purged " + entries + " entries");
            }
         }
      });
      assertEquals(purged.get(), NUM_ENTRIES / 2);
      assertEquals(cs.size(), NUM_ENTRIES / 2);
      cs.clear();
      assertEquals(cs.size(), 0);
   }

   private void fillStore(int count, int size, LifespanProvider provider) {
      Random random = new Random();
      for (int i = 0; i < count; ++i) {
         byte[] data = new byte[size];
         random.nextBytes(data);
         String key = "key" + i;
         Huge value = new Huge(key, data);
         long lifespan = provider == null ? -1 : provider.get();
         MarshalledEntryImpl entry = lifespan < 0 ? createEntry(key, value) : createEntry(key, value, lifespan);
         cs.write(entry);
         if (i % 100 == 0)
            log.info("Writing key " + key);
      }
      log.info("All keys written");
   }

   @Override
   protected Class<?> getEntityClass() {
      return Huge.class;
   }

   private interface LifespanProvider {
      long get();
   }
}
