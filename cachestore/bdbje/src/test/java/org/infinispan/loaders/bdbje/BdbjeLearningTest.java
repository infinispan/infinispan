package org.infinispan.loaders.bdbje;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.mockito.Mockito.*;

/**
 * Learning tests for SleepyCat JE.  Behaviour here is used in BdbjeCacheLoader.  When there are upgrades to bdbje, this
 * test may warrant updating.
 *
 * @author Adrian Cole
 * @since 4.0
 */
@Test(groups = "unit", enabled = true, testName = "loaders.bdbje.BdbjeLearningTest")
public class BdbjeLearningTest extends AbstractInfinispanTest {
   Environment env;

   private static final String CLASS_CATALOG = "java_class_catalog";
   private StoredClassCatalog javaCatalog;

   private static final String STORED_ENTRIES = "storedEntriesDb";
   private Database storedEntriesDb;
   private StoredMap<Object, InternalCacheEntry> cacheMap;

   private String tmpDirectory;

   @BeforeTest
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterTest
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @BeforeMethod
   public void setUp() throws Exception {
      new File(tmpDirectory).mkdirs();
      System.out.println("Opening environment in: " + tmpDirectory);

      EnvironmentConfig envConfig = new EnvironmentConfig();
      envConfig.setTransactional(true);
      envConfig.setAllowCreate(true);

      env = new Environment(new File(tmpDirectory), envConfig);
      DatabaseConfig dbConfig = new DatabaseConfig();
      dbConfig.setTransactional(true);
      dbConfig.setAllowCreate(true);

      Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);

      javaCatalog = new StoredClassCatalog(catalogDb);

      EntryBinding storedEntryKeyBinding =
            new SerialBinding(javaCatalog, Object.class);
      EntryBinding storedEntryValueBinding =
            new InternalCacheEntryBinding(new TestObjectStreamMarshaller());

      storedEntriesDb = env.openDatabase(null, STORED_ENTRIES, dbConfig);

      cacheMap = new StoredMap<Object, InternalCacheEntry>(storedEntriesDb, storedEntryKeyBinding,
                                                           storedEntryValueBinding, true);
   }

   public void testTransactionWorker() throws Exception {
      TransactionRunner runner = new TransactionRunner(env);
      runner.run(new PopulateDatabase());
      runner.run(new PrintDatabase());

   }


   private class PopulateDatabase implements TransactionWorker {
      @Override
      public void doWork()
            throws Exception {
      }
   }

   private class PrintDatabase implements TransactionWorker {
      @Override
      public void doWork()
            throws Exception {
      }
   }


   @AfterMethod
   public void tearDown() throws Exception {
      storedEntriesDb.close();
      javaCatalog.close();
      env.close();

      TestingUtil.recursiveFileRemove(tmpDirectory);
   }


   private void store(InternalCacheEntry se) {
      cacheMap.put(se.getKey(), se);
   }


   private InternalCacheEntry load(Object key) {
      InternalCacheEntry s = cacheMap.get(key);
      if (s == null)
         return null;
      if (!s.isExpired(TIME_SERVICE.wallClockTime()))
         return s;
      else
         cacheMap.remove(key);
      return null;
   }

   private Set loadAll() {
      return new HashSet(cacheMap.values());
   }

   private void purgeExpired() {
      Iterator<Map.Entry<Object, InternalCacheEntry>> i = cacheMap.entrySet().iterator();
      while (i.hasNext()) {
         if (i.next().getValue().isExpired(TIME_SERVICE.wallClockTime()))
            i.remove();
      }
   }

   private static final Log log = LogFactory.getLog(BdbjeLearningTest.class);

   private void toStream(OutputStream outputStream) throws CacheLoaderException {
      ObjectOutputStream oos = null;
      Cursor cursor = null;

      try {
         oos = (outputStream instanceof ObjectOutputStream) ? (ObjectOutputStream) outputStream :
               new ObjectOutputStream(outputStream);
         long recordCount = storedEntriesDb.count();
         log.tracef("writing %s records to stream", recordCount);
         oos.writeLong(recordCount);

         cursor = storedEntriesDb.openCursor(null, null);
         DatabaseEntry key = new DatabaseEntry();
         DatabaseEntry data = new DatabaseEntry();
         while (cursor.getNext(key, data, null) ==
               OperationStatus.SUCCESS) {
            oos.writeObject(key.getData());
            oos.writeObject(data.getData());
         }
      } catch (IOException e) {
         throw new CacheLoaderException("Error writing to object stream", e);
      } catch (DatabaseException e) {
         throw new CacheLoaderException("Error accessing database", e);
      }
      finally {
         if (cursor != null) try {
            cursor.close();
         } catch (DatabaseException e) {
            throw new CacheLoaderException("Error closing cursor", e);
         }
      }

   }

   private void fromStream(InputStream inputStream) throws CacheLoaderException {
      ObjectInputStream ois = null;
      try {
         ois = (inputStream instanceof ObjectInputStream) ? (ObjectInputStream) inputStream :
               new ObjectInputStream(inputStream);
         long recordCount = ois.readLong();
         log.infof("reading %s records from stream", recordCount);
         log.info("clearing all records");
         cacheMap.clear();
         Cursor cursor = null;
         com.sleepycat.je.Transaction txn = env.beginTransaction(null, null);
         try {
            cursor = storedEntriesDb.openCursor(txn, null);
            for (int i = 0; i < recordCount; i++) {
               byte[] keyBytes = (byte[]) ois.readObject();
               byte[] dataBytes = (byte[]) ois.readObject();

               DatabaseEntry key = new DatabaseEntry(keyBytes);
               DatabaseEntry data = new DatabaseEntry(dataBytes);
               cursor.put(key, data);
            }
            cursor.close();
            cursor = null;
            txn.commit();
         } finally {
            if (cursor != null) cursor.close();
         }

      }
      catch (Exception e) {
         throw (e instanceof CacheLoaderException) ? (CacheLoaderException) e :
               new CacheLoaderException("Problems reading from stream", e);
      }
   }

   class StoreTransactionWorker implements TransactionWorker {
      StoreTransactionWorker(InternalCacheEntry entry) {
         this.entry = entry;
      }

      private InternalCacheEntry entry;

      @Override
      public void doWork() throws Exception {
         store(entry);
      }
   }

   class ClearTransactionWorker implements TransactionWorker {

      @Override
      public void doWork() throws Exception {
         cacheMap.clear();
      }
   }

   class RemoveTransactionWorker implements TransactionWorker {
      RemoveTransactionWorker(Object key) {
         this.key = key;
      }

      Object key;

      @Override
      public void doWork() throws Exception {
         cacheMap.remove(key);
      }
   }

   class PurgeExpiredTransactionWorker implements TransactionWorker {
      @Override
      public void doWork() throws Exception {
         purgeExpired();
      }
   }

   class ModificationsTransactionWorker implements TransactionWorker {
      private List<? extends Modification> mods;

      ModificationsTransactionWorker(List<? extends Modification> mods) {
         this.mods = mods;
      }

      @Override
      public void doWork() throws Exception {
         for (Modification modification : mods)
            switch (modification.getType()) {
               case STORE:
                  Store s = (Store) modification;
                  store(s.getStoredEntry());
                  break;
               case CLEAR:
                  cacheMap.clear();
                  break;
               case REMOVE:
                  Remove r = (Remove) modification;
                  cacheMap.remove(r.getKey());
                  break;
               case PURGE_EXPIRED:
                  purgeExpired();
                  break;
               default:
                  throw new IllegalArgumentException("Unknown modification type " + modification.getType());
            }
      }
   }


   private void prepare(List<Modification> mods, Transaction tx, boolean isOnePhase) throws CacheLoaderException {
      if (isOnePhase) {
         TransactionRunner runner = new TransactionRunner(env);
         try {
            runner.run(new ModificationsTransactionWorker(mods));
         } catch (Exception e) {
            e.printStackTrace();
         }
      } else {
         PreparableTransactionRunner runner = new PreparableTransactionRunner(env);
         com.sleepycat.je.Transaction txn = null;
         try {
            runner.prepare(new ModificationsTransactionWorker(mods));
            txn = CurrentTransaction.getInstance(env).getTransaction();
            txnMap.put(tx, txn);
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

   }

   Map<Transaction, com.sleepycat.je.Transaction> txnMap = new HashMap<Transaction, com.sleepycat.je.Transaction>();

   private void commit(Transaction tx) {
      com.sleepycat.je.Transaction txn = txnMap.remove(tx);
      CurrentTransaction currentTransaction = CurrentTransaction.getInstance(env);
      if (txn != null) {
         if (currentTransaction.getTransaction() == txn) {
            try {
               currentTransaction.commitTransaction();
            } catch (DatabaseException e) {
               e.printStackTrace();
            }
         } else {
            log.error("Transactions must be committed on the same thread");
         }
      }
   }

   private void rollback(Transaction tx) {
      com.sleepycat.je.Transaction txn = txnMap.remove(tx);
      CurrentTransaction currentTransaction = CurrentTransaction.getInstance(env);
      if (txn != null) {
         if (currentTransaction.getTransaction() == txn) {
            try {
               currentTransaction.abortTransaction();
            } catch (DatabaseException e) {
               e.printStackTrace();
            }
         } else {
            log.error("Transactions must be committed on the same thread");
         }
      }
   }

   public void testLoadAndStore() throws InterruptedException, CacheLoaderException {
      assert !cacheMap.containsKey("k");
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", "v");
      store(se);

      assert load("k").getValue().equals("v");
      assert load("k").getLifespan() == -1;
      assert !load("k").isExpired(TIME_SERVICE.wallClockTime());
      assert cacheMap.containsKey("k");

      long lifespan = 120000;
      se = TestInternalCacheEntryFactory.create("k", "v", lifespan);
      store(se);

      assert load("k").getValue().equals("v");
      assert load("k").getLifespan() == lifespan;
      assert !load("k").isExpired(TIME_SERVICE.wallClockTime());
      assert cacheMap.containsKey("k");

      lifespan = 1;
      se = TestInternalCacheEntryFactory.create("k", "v", lifespan);
      store(se);
      Thread.sleep(100);
      assert se.isExpired(TIME_SERVICE.wallClockTime());
      assert load("k") == null;
      assert !cacheMap.containsKey("k");
   }


   public void testOnePhaseCommit() throws CacheLoaderException {
      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      Transaction tx = mock(Transaction.class);
      prepare(mods, tx, true);

      Set s = loadAll();

      assert load("k2").getValue().equals("v2");
      assert !cacheMap.containsKey("k1");

      cacheMap.clear();

      mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));

      prepare(mods, tx, true);
      assert !cacheMap.containsKey("k1");
      assert !cacheMap.containsKey("k2");
      assert cacheMap.containsKey("k3");
   }


   public void testTwoPhaseCommit() throws Throwable {
      final List<Throwable> throwables = new ArrayList<Throwable>();
      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      Transaction tx = mock(Transaction.class);
      prepare(mods, tx, false);


      Thread gets1 = new Thread(
            new Runnable() {
               @Override
               public void run() {
                  try {
                     assert load("k2").getValue().equals("v2");
                     assert !cacheMap.containsKey("k1");
                  } catch (Throwable e) {
                     throwables.add(e);
                  }
               }
            }
      );

      gets1.start();
      commit(tx);

      gets1.join();

      if (!throwables.isEmpty()) throw throwables.get(0);


      cacheMap.clear();

      mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));

      prepare(mods, tx, false);

      Thread gets2 = new Thread(
            new Runnable() {
               @Override
               public void run() {
                  try {
                     assert !cacheMap.containsKey("k1");
                     assert !cacheMap.containsKey("k2");
                     assert cacheMap.containsKey("k3");

                  } catch (Throwable e) {
                     throwables.add(e);
                  }
               }
            }
      );

      gets2.start();


      commit(tx);
      gets2.join();

      if (!throwables.isEmpty()) throw throwables.get(0);
      assert !cacheMap.containsKey("k1");
      assert !cacheMap.containsKey("k2");
      assert cacheMap.containsKey("k3");
   }


   public void testRollback() throws Throwable {

      store(TestInternalCacheEntryFactory.create("old", "old"));


      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      mods.add(new Remove("old"));
      Transaction tx = mock(Transaction.class);
      prepare(mods, tx, false);

      rollback(tx);

      assert !cacheMap.containsKey("k1");
      assert !cacheMap.containsKey("k2");
      assert cacheMap.containsKey("old");

      mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));

      prepare(mods, tx, false);

      rollback(tx);

      assert !cacheMap.containsKey("k1");
      assert !cacheMap.containsKey("k2");
      assert !cacheMap.containsKey("k3");
      assert cacheMap.containsKey("old");
   }


   public void testCommitAndRollbackWithoutPrepare() throws CacheLoaderException {
      store(TestInternalCacheEntryFactory.create("old", "old"));
      Transaction tx = mock(Transaction.class);
      commit(tx);
      store(TestInternalCacheEntryFactory.create("old", "old"));
      rollback(tx);

      assert cacheMap.containsKey("old");
   }

   public void testPreload() throws CacheLoaderException {
      store(TestInternalCacheEntryFactory.create("k1", "v1"));
      store(TestInternalCacheEntryFactory.create("k2", "v2"));
      store(TestInternalCacheEntryFactory.create("k3", "v3"));

      Set<InternalCacheEntry> set = loadAll();

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   public void testPurgeExpired() throws Exception {
      long now = System.currentTimeMillis();
      long lifespan = 1000;
      store(TestInternalCacheEntryFactory.create("k1", "v1", lifespan));
      store(TestInternalCacheEntryFactory.create("k2", "v2", lifespan));
      store(TestInternalCacheEntryFactory.create("k3", "v3", lifespan));

      assert cacheMap.containsKey("k1");
      assert cacheMap.containsKey("k2");
      assert cacheMap.containsKey("k3");
      Thread.sleep(lifespan + 100);
      purgeExpired();
      assert !cacheMap.containsKey("k1");
      assert !cacheMap.containsKey("k2");
      assert !cacheMap.containsKey("k3");
   }


   public void testStreamingAPI() throws IOException, ClassNotFoundException, CacheLoaderException {
      store(TestInternalCacheEntryFactory.create("k1", "v1"));
      store(TestInternalCacheEntryFactory.create("k2", "v2"));
      store(TestInternalCacheEntryFactory.create("k3", "v3"));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      toStream(out);
      out.close();
      cacheMap.clear();
      fromStream(new ByteArrayInputStream(out.toByteArray()));

      Set<InternalCacheEntry> set = loadAll();

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }


   public void testStreamingAPIReusingStreams() throws IOException, ClassNotFoundException, CacheLoaderException {
      store(TestInternalCacheEntryFactory.create("k1", "v1"));
      store(TestInternalCacheEntryFactory.create("k2", "v2"));
      store(TestInternalCacheEntryFactory.create("k3", "v3"));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] dummyStartBytes = {1, 2, 3, 4, 5, 6, 7, 8};
      byte[] dummyEndBytes = {8, 7, 6, 5, 4, 3, 2, 1};
      out.write(dummyStartBytes);
      toStream(out);
      out.write(dummyEndBytes);
      out.close();
      cacheMap.clear();

      // first pop the start bytes
      byte[] dummy = new byte[8];
      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      int bytesRead = in.read(dummy, 0, 8);
      assert bytesRead == 8;
      for (int i = 1; i < 9; i++) assert dummy[i - 1] == i : "Start byte stream corrupted!";
      fromStream(in);
      bytesRead = in.read(dummy, 0, 8);
      assert bytesRead == 8;
      for (int i = 8; i > 0; i--) assert dummy[8 - i] == i : "Start byte stream corrupted!";

      Set<InternalCacheEntry> set = loadAll();

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   @Test(enabled = false)
   public void testConcurrency() throws Throwable {
      int numThreads = 3;
      final int loops = 500;
      final String[] keys = new String[10];
      final String[] values = new String[10];
      for (int i = 0; i < 10; i++) keys[i] = "k" + i;
      for (int i = 0; i < 10; i++) values[i] = "v" + i;


      final Random r = new Random();
      final List<Throwable> throwables = new LinkedList<Throwable>();

      final Runnable store = new Runnable() {
         @Override
         public void run() {
            try {
               int randomInt = r.nextInt(10);
               store(TestInternalCacheEntryFactory.create(keys[randomInt], values[randomInt]));
            } catch (Throwable e) {
               throwables.add(e);
            }
         }
      };

      final Runnable remove = new Runnable() {
         @Override
         public void run() {
            try {
               cacheMap.remove(keys[r.nextInt(10)]);
            } catch (Throwable e) {
               throwables.add(e);
            }
         }
      };

      final Runnable get = new Runnable() {
         @Override
         public void run() {
            try {
               int randomInt = r.nextInt(10);
               InternalCacheEntry se = load(keys[randomInt]);
               assert se == null || se.getValue().equals(values[randomInt]);
               loadAll();
            } catch (Throwable e) {
               throwables.add(e);
            }
         }
      };

      Thread[] threads = new Thread[numThreads];

      for (int i = 0; i < numThreads; i++) {
         threads[i] = new Thread(getClass().getSimpleName() + "-" + i) {
            @Override
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

      if (!throwables.isEmpty()) throw throwables.get(0);
   }


}

