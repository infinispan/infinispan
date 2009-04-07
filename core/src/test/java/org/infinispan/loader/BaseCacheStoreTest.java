package org.infinispan.loader;

import org.easymock.EasyMock;
import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loader.modifications.Clear;
import org.infinispan.loader.modifications.Modification;
import org.infinispan.loader.modifications.Remove;
import org.infinispan.loader.modifications.Store;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.ObjectStreamMarshaller;
import org.infinispan.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

@SuppressWarnings("unchecked")
// this needs to be here for the test to run in an IDE
@Test(groups = "unit", testName = "loader.BaseCacheStoreTest")
public abstract class BaseCacheStoreTest {

   protected abstract CacheStore createCacheStore() throws Exception;

   protected CacheStore cs;

   @BeforeMethod
   public void setUp() throws Exception {
      try {
         cs = createCacheStore();
      } catch (Exception e) {
         //in IDEs this won't be printed which makes debugging harder
         e.printStackTrace();
         throw e;
      }
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      if (cs != null) {
         cs.clear();
         cs.stop();
      }
      cs = null;
   }

   @AfterMethod
   public void assertNoLocksHeld() {
      //doesn't really make sense to add a subclass for this check only
      if (cs instanceof LockSupportCacheStore) {
         assert ((LockSupportCacheStore) cs).getTotalLockCount() == 0;
      }
   }

   /**
    * @return a mock cache for use with the cache store impls
    */
   protected Cache getCache() {
      Cache c = EasyMock.createNiceMock(Cache.class);
      EasyMock.expect(c.getName()).andReturn("mockCache-" + getClass().getName()).anyTimes();
      EasyMock.replay(c);
      return c;
   }

   /**
    * @return a mock marshaller for use with the cache store impls
    */
   protected Marshaller getMarshaller() {
      return new ObjectStreamMarshaller();
   }

   public void testLoadAndStoreImmortal() throws InterruptedException, CacheLoaderException {
      assert !cs.containsKey("k");
      InternalCacheEntry se = InternalEntryFactory.create("k", "v");
      cs.store(se);

      assert cs.load("k").getValue().equals("v");
      assert cs.load("k").getLifespan() == -1;
      assert cs.load("k").getMaxIdle() == -1;
      assert !cs.load("k").isExpired();
      assert cs.containsKey("k");
   }

   public void testLoadAndStoreWithLifespan() throws InterruptedException, CacheLoaderException {
      assert !cs.containsKey("k");

      long lifespan = 120000;
      InternalCacheEntry se = InternalEntryFactory.create("k", "v", lifespan);
      cs.store(se);

      assert cs.load("k").getValue().equals("v");
      assert cs.load("k").getLifespan() == lifespan;
      assert cs.load("k").getMaxIdle() == -1;
      assert !cs.load("k").isExpired();
      assert cs.containsKey("k");

      lifespan = 1;
      se = InternalEntryFactory.create("k", "v", lifespan);
      cs.store(se);
      Thread.sleep(100);
      assert se.isExpired();
      assert cs.load("k") == null;
      assert !cs.containsKey("k");
   }

   public void testLoadAndStoreWithIdle() throws InterruptedException, CacheLoaderException {
      assert !cs.containsKey("k");

      long idle = 120000;
      InternalCacheEntry se = InternalEntryFactory.create("k", "v", -1, idle);
      cs.store(se);

      assert cs.load("k").getValue().equals("v");
      assert cs.load("k").getLifespan() == -1;
      assert cs.load("k").getMaxIdle() == idle;
      assert !cs.load("k").isExpired();
      assert cs.containsKey("k");

      idle = 1;
      se = InternalEntryFactory.create("k", "v", -1, idle);
      cs.store(se);
      Thread.sleep(100);
      assert se.isExpired();
      assert cs.load("k") == null;
      assert !cs.containsKey("k");
   }

   public void testLoadAndStoreWithLifespanAndIdle() throws InterruptedException, CacheLoaderException {
      assert !cs.containsKey("k");

      long lifespan = 200000;
      long idle = 120000;
      InternalCacheEntry se = InternalEntryFactory.create("k", "v", lifespan, idle);
      cs.store(se);

      assert cs.load("k").getValue().equals("v");
      assert cs.load("k").getLifespan() == lifespan;
      assert cs.load("k").getMaxIdle() == idle;
      assert !cs.load("k").isExpired();
      assert cs.containsKey("k");

      idle = 1;
      se = InternalEntryFactory.create("k", "v", lifespan, idle);
      cs.store(se);
      Thread.sleep(100);
      assert se.isExpired();
      assert cs.load("k") == null;
      assert !cs.containsKey("k");
   }

   public void testStopStartDoesntNukeValues() throws InterruptedException, CacheLoaderException {
      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");

      long lifespan = 1;
      long idle = 1;
      InternalCacheEntry se1 = InternalEntryFactory.create("k1", "v1", lifespan);
      InternalCacheEntry se2 = InternalEntryFactory.create("k2", "v2");
      InternalCacheEntry se3 = InternalEntryFactory.create("k3", "v3", -1, idle);
      InternalCacheEntry se4 = InternalEntryFactory.create("k4", "v4", lifespan, idle);

      cs.store(se1);
      cs.store(se2);
      cs.store(se3);
      cs.store(se4);
      Thread.sleep(100);
      cs.stop();
      cs.start();
      assert se1.isExpired();
      assert cs.load("k1") == null;
      assert !cs.containsKey("k1");
      assert cs.load("k2") != null;
      assert cs.containsKey("k2");
      assert cs.load("k2").getValue().equals("v2");
      assert se3.isExpired();
      assert cs.load("k3") == null;
      assert !cs.containsKey("k3");
      assert se3.isExpired();
      assert cs.load("k3") == null;
      assert !cs.containsKey("k3");
   }


   public void testOnePhaseCommit() throws CacheLoaderException {
      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      Transaction tx = EasyMock.createNiceMock(Transaction.class);
      cs.prepare(mods, tx, true);

      assert cs.load("k2").getValue().equals("v2");
      assert !cs.containsKey("k1");

      cs.clear();

      mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(InternalEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, true);
      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("k3");
   }

   public void testTwoPhaseCommit() throws CacheLoaderException {
      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      Transaction tx = EasyMock.createNiceMock(Transaction.class);
      cs.prepare(mods, tx, false);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");

      cs.commit(tx);

      assert cs.load("k2").getValue().equals("v2");
      assert !cs.containsKey("k1");

      cs.clear();

      mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(InternalEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, false);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");

      cs.commit(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("k3");
   }


   public void testRollback() throws CacheLoaderException {

      cs.store(InternalEntryFactory.create("old", "old"));

      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      mods.add(new Remove("old"));
      Transaction tx = EasyMock.createNiceMock(Transaction.class);
      cs.prepare(mods, tx, false);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("old");

      cs.rollback(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("old");

      mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(InternalEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, false);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");

      cs.rollback(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
      assert cs.containsKey("old");
   }

   public void testRollbackFromADifferentThreadReusingTransactionKey() throws CacheLoaderException, InterruptedException {

      cs.store(InternalEntryFactory.create("old", "old"));

      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      mods.add(new Remove("old"));
      final Transaction tx = EasyMock.createNiceMock(Transaction.class);
      cs.prepare(mods, tx, false);

      Thread t = new Thread(new Runnable() {
         public void run() {
            cs.rollback(tx);
         }
      });

      t.start();
      t.join();

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("old");

      mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(InternalEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, false);

      Thread t2 = new Thread(new Runnable() {
         public void run() {
            cs.rollback(tx);
         }
      });

      t2.start();
      t2.join();
      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
      assert cs.containsKey("old");
   }

   public void testCommitAndRollbackWithoutPrepare() throws CacheLoaderException {
      cs.store(InternalEntryFactory.create("old", "old"));
      Transaction tx = EasyMock.createNiceMock(Transaction.class);
      cs.commit(tx);
      cs.store(InternalEntryFactory.create("old", "old"));
      cs.rollback(tx);

      assert cs.containsKey("old");
   }

   public void testPreload() throws CacheLoaderException {
      cs.store(InternalEntryFactory.create("k1", "v1"));
      cs.store(InternalEntryFactory.create("k2", "v2"));
      cs.store(InternalEntryFactory.create("k3", "v3"));

      Set<InternalCacheEntry> set = cs.loadAll();

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   @Test
   public void testStoreAndRemoveAll() throws CacheLoaderException {
      cs.store(InternalEntryFactory.create("k1", "v1"));
      cs.store(InternalEntryFactory.create("k2", "v2"));
      cs.store(InternalEntryFactory.create("k3", "v3"));
      cs.store(InternalEntryFactory.create("k4", "v4"));


      Set<InternalCacheEntry> set = cs.loadAll();

      assert set.size() == 4;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();

      Set toRemove = new HashSet();
      toRemove.add("k1");
      toRemove.add("k2");
      toRemove.add("k3");
      cs.removeAll(toRemove);

      set = cs.loadAll();
      assert set.size() == 1;
      set.remove("k4");
      assert expected.isEmpty();
   }

   public void testPurgeExpired() throws Exception {
      long lifespan = 1000;
      long idle = 500;
      cs.store(InternalEntryFactory.create("k1", "v1", lifespan));
      cs.store(InternalEntryFactory.create("k2", "v2", -1, idle));
      cs.store(InternalEntryFactory.create("k3", "v3", lifespan, idle));
      assert cs.containsKey("k1");
      assert cs.containsKey("k2");
      assert cs.containsKey("k3");
      Thread.sleep(lifespan + 100);
      cs.purgeExpired();
      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
   }

   public void testStreamingAPI() throws IOException, ClassNotFoundException, CacheLoaderException {
      cs.store(InternalEntryFactory.create("k1", "v1"));
      cs.store(InternalEntryFactory.create("k2", "v2"));
      cs.store(InternalEntryFactory.create("k3", "v3"));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(out);
      cs.toStream(new UnclosableObjectOutputStream(oos));
      oos.flush();
      oos.close();
      out.close();
      cs.clear();
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
      cs.fromStream(new UnclosableObjectInputStream(ois));

      Set<InternalCacheEntry> set = cs.loadAll();

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   public void testStreamingAPIReusingStreams() throws IOException, ClassNotFoundException, CacheLoaderException {
      cs.store(InternalEntryFactory.create("k1", "v1"));
      cs.store(InternalEntryFactory.create("k2", "v2"));
      cs.store(InternalEntryFactory.create("k3", "v3"));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] dummyStartBytes = {1, 2, 3, 4, 5, 6, 7, 8};
      byte[] dummyEndBytes = {8, 7, 6, 5, 4, 3, 2, 1};
      out.write(dummyStartBytes);
      ObjectOutputStream oos = new ObjectOutputStream(out);
      cs.toStream(new UnclosableObjectOutputStream(oos));
      oos.flush();
      oos.close();
      out.write(dummyEndBytes);
      out.close();
      cs.clear();

      // first pop the start bytes
      byte[] dummy = new byte[8];
      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      int bytesRead = in.read(dummy, 0, 8);
      assert bytesRead == 8;
      for (int i = 1; i < 9; i++) assert dummy[i - 1] == i : "Start byte stream corrupted!";
      cs.fromStream(new UnclosableObjectInputStream(new ObjectInputStream(in)));
      bytesRead = in.read(dummy, 0, 8);
      assert bytesRead == 8;
      for (int i = 8; i > 0; i--) assert dummy[8 - i] == i : "Start byte stream corrupted!";

      Set<InternalCacheEntry> set = cs.loadAll();

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   public void testConfigFile() throws Exception {
      Class<? extends CacheLoaderConfig> cfgClass = cs.getConfigurationClass();
      CacheLoaderConfig clc = Util.getInstance(cfgClass);
      assert clc.getCacheLoaderClassName().equals(cs.getClass().getName()) : "Cache loader doesn't provide a proper configuration type that is capable of creating the loader!";
   }

   public void testConcurrency() throws Exception {
      int numThreads = 3;
      final int loops = 500;
      final String[] keys = new String[10];
      final String[] values = new String[10];
      for (int i = 0; i < 10; i++) keys[i] = "k" + i;
      for (int i = 0; i < 10; i++) values[i] = "v" + i;


      final Random r = new Random();
      final List<Exception> exceptions = new LinkedList<Exception>();

      final Runnable store = new Runnable() {
         public void run() {
            try {
               int randomInt = r.nextInt(10);
               cs.store(InternalEntryFactory.create(keys[randomInt], values[randomInt]));
            } catch (Exception e) {
               exceptions.add(e);
            }
         }
      };

      final Runnable remove = new Runnable() {
         public void run() {
            try {
               cs.remove(keys[r.nextInt(10)]);
            } catch (Exception e) {
               exceptions.add(e);
            }
         }
      };

      final Runnable get = new Runnable() {
         public void run() {
            try {
               int randomInt = r.nextInt(10);
               InternalCacheEntry se = cs.load(keys[randomInt]);
               assert se == null || se.getValue().equals(values[randomInt]);
               cs.loadAll();
            } catch (Exception e) {
               exceptions.add(e);
            }
         }
      };

      Thread[] threads = new Thread[numThreads];

      for (int i = 0; i < numThreads; i++) {
         threads[i] = new Thread(getClass().getSimpleName() + "-" + i) {
            public void run() {
               for (int i = 0; i < loops; i++) {
                  store.run();
                  remove.run();
                  get.run();
               }
            }
         };
      }

      for (Thread t : threads) t.start();
      for (Thread t : threads) t.join();

      if (!exceptions.isEmpty()) throw exceptions.get(0);
   }
}
