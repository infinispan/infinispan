package org.horizon.loader.bdbje;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.util.RuntimeExceptionWrapper;
import static org.easymock.classextension.EasyMock.*;
import org.horizon.Cache;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.StoredEntry;
import org.horizon.loader.modifications.Store;
import org.horizon.util.ReflectionUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.lang.ref.WeakReference;
import java.util.Collections;

/**
 * @author Adrian Cole
 * @version $Id: $
 * @since 4.0
 */
@Test(groups = "unit", enabled = true, testName = "loader.bdbje.BdbjeCacheStoreTest")
public class BdbjeCacheStoreTest {
   private BdbjeCacheStore cs;
   private BdbjeCacheStoreConfig cfg;
   private BdbjeResourceFactory factory;
   private Cache cache;
   private Environment env;
   private Database cacheDb;
   private Database catalogDb;
   private StoredClassCatalog catalog;
   private StoredMap cacheMap;
   private PreparableTransactionRunner runner;
   private CurrentTransaction currentTransaction;

   private class MockBdbjeResourceFactory extends BdbjeResourceFactory {

      @Override
      public PreparableTransactionRunner createPreparableTransactionRunner(Environment env) {
         return runner;
      }

      @Override
      public CurrentTransaction createCurrentTransaction(Environment env) {
         return currentTransaction;
      }

      @Override
      public Environment createEnvironment(File envLocation) throws DatabaseException {
         return env;
      }

      @Override
      public StoredClassCatalog createStoredClassCatalog(Database catalogDb) throws DatabaseException {
         return catalog;
      }

      @Override
      public Database createDatabase(Environment env, String name) throws DatabaseException {
         if (name.equals(cfg.getCacheDbName()))
            return cacheDb;
         else
            return catalogDb;
      }

      @Override
      public StoredMap createStoredMapViewOfDatabase(Database database, StoredClassCatalog classCatalog) throws DatabaseException {
         return cacheMap;
      }

      public MockBdbjeResourceFactory(BdbjeCacheStoreConfig config) {
         super(config);
      }
   }

   @BeforeMethod
   public void setUp() throws Exception {
      cfg = new BdbjeCacheStoreConfig();
      factory = new MockBdbjeResourceFactory(cfg);
      cache = createMock(Cache.class);
      cs = new BdbjeCacheStore();
      env = createMock(Environment.class);
      cacheDb = createMock(Database.class);
      catalogDb = createMock(Database.class);
      catalog = createMock(StoredClassCatalog.class);
      cacheMap = createMock(StoredMap.class);
      currentTransaction = createMock(CurrentTransaction.class);
      WeakReference<Environment> envRef = new WeakReference<Environment>(env);
      ReflectionUtil.setValue(currentTransaction,"envRef",envRef);
      ThreadLocal localTrans = new ThreadLocal();
      ReflectionUtil.setValue(currentTransaction,"localTrans",localTrans);
      runner = createMock(PreparableTransactionRunner.class);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      runner = null;
      currentTransaction = null;
      cacheMap = null;
      catalogDb = null;
      cacheDb = null;
      env = null;
      factory = null;
      cache = null;
      cfg = null;
      cs = null;
   }

   void start() throws DatabaseException, CacheLoaderException {
      cs.init(cfg, factory, cache);
      expect(cache.getName()).andReturn("cache");
   }

   @Test
   public void testGetConfigurationClass() throws Exception {
      replayAll();
      assert cs.getConfigurationClass().equals(BdbjeCacheStoreConfig.class);
      verifyAll();
   }

   void replayAll() throws Exception {
      replay(runner);
      replay(currentTransaction);
      replay(cacheMap);
      replay(catalog);
      replay(catalogDb);
      replay(cacheDb);
      replay(env);
      replay(cache);
   }

   void verifyAll() throws Exception {
      verify(runner);
      verify(currentTransaction);
      verify(cacheMap);
      verify(catalog);
      verify(catalogDb);
      verify(env);
      verify(cache);
   }

   @Test
   public void testInitNoMock() throws Exception {
      replayAll();
      cs.init(cfg, cache, null);
      assert cfg.equals(ReflectionUtil.getValue(cs, "cfg"));
      assert cache.equals(ReflectionUtil.getValue(cs, "cache"));
      assert ReflectionUtil.getValue(cs, "factory") instanceof BdbjeResourceFactory;
      verifyAll();
   }

   @Test
   void testExceptionClosingCacheDatabaseDoesntPreventEnvironmentFromClosing() throws Exception {
      start();
      cacheDb.close();
      expectLastCall().andThrow(new DatabaseException());
      catalog.close();
      env.close();
      replayAll();
      cs.start();
      cs.stop();

      verifyAll();
   }

   @Test
   void testExceptionClosingCatalogDoesntPreventEnvironmentFromClosing() throws Exception {
      start();
      cacheDb.close();
      catalog.close();
      expectLastCall().andThrow(new DatabaseException());
      env.close();
      replayAll();
      cs.start();
      cs.stop();
      verifyAll();
   }

   @Test(expectedExceptions = CacheLoaderException.class)
   void testExceptionClosingEnvironment() throws Exception {
      start();
      cacheDb.close();
      catalog.close();
      env.close();
      expectLastCall().andThrow(new DatabaseException());
      replayAll();
      cs.start();
      cs.stop();
      verifyAll();
   }


   @Test(expectedExceptions = CacheLoaderException.class)
   void testThrowsCorrectExceptionOnStartForDatabaseException() throws Exception {
      factory = new MockBdbjeResourceFactory(cfg) {
         @Override
         public StoredClassCatalog createStoredClassCatalog(Database catalogDb) throws DatabaseException {
            throw new DatabaseException();
         }
      };
      start();
      replayAll();
      cs.start();

   }

   @Test(expectedExceptions = CacheLoaderException.class)
   void testEnvironmentDirectoryExistsButNotAFile() throws Exception {
      File file = createMock(File.class);
      expect(file.exists()).andReturn(true);
      expect(file.isDirectory()).andReturn(false);
      replay(file);
      cs.verifyOrCreateEnvironmentDirectory(file);
   }

   @Test(expectedExceptions = CacheLoaderException.class)
   void testCantCreateEnvironmentDirectory() throws Exception {
      File file = createMock(File.class);
      expect(file.exists()).andReturn(false);
      expect(file.mkdirs()).andReturn(false);
      replay(file);
      cs.verifyOrCreateEnvironmentDirectory(file);
   }

   @Test
   void testCanCreateEnvironmentDirectory() throws Exception {
      File file = createMock(File.class);
      expect(file.exists()).andReturn(false);
      expect(file.mkdirs()).andReturn(true);
      expect(file.isDirectory()).andReturn(true);
      replay(file);
      assert file.equals(cs.verifyOrCreateEnvironmentDirectory(file));
   }

   @Test
   public void testNoExceptionOnRollback() throws Exception {
      start();
      Transaction tx = createMock(Transaction.class);
      replayAll();
      cs.start();
      cs.rollback(tx);
      verifyAll();
   }

   @Test
   protected void testApplyModificationsThrowsOriginalDatabaseException() throws Exception {
      start();
      DatabaseException ex = new DatabaseException();
      runner.run(isA(TransactionWorker.class));
      expectLastCall().andThrow(new RuntimeExceptionWrapper(ex));
      replayAll();
      cs.start();
      try {
         cs.applyModifications(Collections.singletonList(new Store(new StoredEntry("k", "v"))));
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         verifyAll();
         return;
      }
      assert false : "should have returned";

   }

   @Test
   protected void testCommitThrowsOriginalDatabaseException() throws Exception {
      start();
      DatabaseException ex = new DatabaseException();
      com.sleepycat.je.Transaction txn = createMock(com.sleepycat.je.Transaction.class);
      expect(currentTransaction.beginTransaction(null)).andReturn(txn);
      runner.prepare(isA(TransactionWorker.class));
      txn.commit();
      expectLastCall().andThrow(new RuntimeExceptionWrapper(ex));
      replayAll();
      replay(txn);
      cs.start();
      try {
         txn = currentTransaction.beginTransaction(null);
         Transaction t = createMock(Transaction.class);
         cs.prepare(Collections.singletonList(new Store(new StoredEntry("k", "v"))), t,false);
         cs.commit(t);
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         verifyAll();
         return;
      }
      assert false : "should have returned";

   }
            
   @Test
   protected void testPrepareThrowsOriginalDatabaseException() throws Exception {
      start();
      DatabaseException ex = new DatabaseException();
      runner.prepare(isA(TransactionWorker.class));
      expectLastCall().andThrow(new RuntimeExceptionWrapper(ex));
      replayAll();
      cs.start();
      try {
         cs.prepare(Collections.singletonList(new Store(new StoredEntry("k", "v"))), createMock(Transaction.class),false);
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         verifyAll();
         return;
      }
      assert false : "should have returned";

   }

   @Test
   void testClearOnAbortFromStream() throws Exception {
      start();
      StoredEntry entry = new StoredEntry();
      expect(cacheMap.put(entry.getKey(), entry)).andReturn(null);
      ObjectInput ois = createMock(ObjectInput.class);
      expect(ois.readLong()).andReturn(new Long(1));
      com.sleepycat.je.Transaction txn = createMock( com.sleepycat.je.Transaction.class);
      expect(currentTransaction.beginTransaction(null)).andReturn(txn);
      cacheMap.clear();
      Cursor cursor = createMock(Cursor.class);
      expect(cacheDb.openCursor(txn, null)).andReturn(cursor);
      IOException ex = new IOException();
      expect(ois.readObject()).andReturn(new byte[0]);
      expectLastCall().andThrow(ex);
      txn.abort();
      cacheMap.clear();
      replay(ois);
      replay(txn);
      replayAll();
      cs.start();
      try {
         cs.store(entry);
         cs.fromStream(ois);
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         verifyAll();
         verify(ois);
         verify(txn);
         return;
      }
      assert false : "should have returned";
   }
}
