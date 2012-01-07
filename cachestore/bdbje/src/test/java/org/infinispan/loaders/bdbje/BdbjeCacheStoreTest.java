/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.bdbje;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.util.RuntimeExceptionWrapper;
import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.ReflectionUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Properties;

import static org.easymock.classextension.EasyMock.*;

/**
 * @author Adrian Cole
 * @since 4.0
 */
@Test(groups = "unit", testName = "loaders.bdbje.BdbjeCacheStoreTest")
public class BdbjeCacheStoreTest {
   private BdbjeCacheStore cs;
   private BdbjeCacheStoreConfig cfg;
   private BdbjeResourceFactory factory;
   private Cache cache;
   private Environment env;
   private Database cacheDb;
   private Database catalogDb;
   private Database expiryDb;

   private StoredClassCatalog catalog;
   private StoredMap cacheMap;
   private StoredSortedMap expiryMap;

   private PreparableTransactionRunner runner;
   private CurrentTransaction currentTransaction;
   private TransactionFactory gtf;

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
      public Environment createEnvironment(File envLocation, Properties environmentProperties) throws DatabaseException {
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
         else if (name.equals(cfg.getCatalogDbName()))
            return catalogDb;
         else if (name.equals(cfg.getExpiryDbName()))
            return expiryDb;
         else throw new IllegalStateException("Unknown name:" + name);
      }

      @Override
      public StoredMap createStoredMapViewOfDatabase(Database database, StoredClassCatalog classCatalog, StreamingMarshaller m) throws DatabaseException {
         return cacheMap;
      }

      @Override
      public StoredSortedMap<Long, Object> createStoredSortedMapForKeyExpiry(Database database, StoredClassCatalog classCatalog, StreamingMarshaller marshaller) throws DatabaseException {
         return expiryMap;
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
      expiryDb = createMock(Database.class);
      catalog = createMock(StoredClassCatalog.class);
      cacheMap = createMock(StoredMap.class);
      expiryMap = createMock(StoredSortedMap.class);
      currentTransaction = createMock(CurrentTransaction.class);
      gtf = new TransactionFactory();
      gtf.init(false, false, true);
      WeakReference<Environment> envRef = new WeakReference<Environment>(env);
      ReflectionUtil.setValue(currentTransaction, "envRef", envRef);
      ThreadLocal localTrans = new ThreadLocal();
      ReflectionUtil.setValue(currentTransaction, "localTrans", localTrans);
      runner = createMock(PreparableTransactionRunner.class);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      runner = null;
      currentTransaction = null;
      cacheMap = null;
      catalogDb = null;
      expiryDb = null;
      cacheDb = null;
      env = null;
      factory = null;
      cache = null;
      cfg = null;
      cs = null;
      gtf = null;
   }

   void start() throws DatabaseException, CacheLoaderException {
      cs.init(cfg, factory, cache, new TestObjectStreamMarshaller());
      expect(cache.getName()).andReturn("cache");
      expect(cache.getConfiguration()).andReturn(null).anyTimes();
   }

   public void testGetConfigurationClass() throws Exception {
      replayAll();
      assert cs.getConfigurationClass().equals(BdbjeCacheStoreConfig.class);
      verifyAll();
   }

   void replayAll() throws Exception {
      replay(runner);
      replay(currentTransaction);
      replay(cacheMap);
      replay(expiryMap);
      replay(catalog);
      replay(catalogDb);
      replay(expiryDb);
      replay(cacheDb);
      replay(env);
      replay(cache);
   }

   void verifyAll() throws Exception {
      verify(runner);
      verify(currentTransaction);
      verify(cacheMap);
      verify(expiryMap);
      verify(catalog);
      verify(catalogDb);
      verify(expiryDb);
      verify(env);
      verify(cache);
   }

   public void testInitNoMock() throws Exception {
      replayAll();
      cs.init(cfg, cache, null);
      assert cfg.equals(ReflectionUtil.getValue(cs, "cfg"));
      assert cache.equals(ReflectionUtil.getValue(cs, "cache"));
      assert ReflectionUtil.getValue(cs, "factory") instanceof BdbjeResourceFactory;
      verifyAll();
   }

   public void testExceptionClosingCacheDatabaseDoesNotPreventEnvironmentFromClosing() throws Exception {
      start();
      cacheDb.close();
      expiryDb.close();
      expectLastCall().andThrow(new DatabaseException("Dummy"){});
      catalog.close();
      env.close();
      replayAll();
      cs.start();
      cs.stop();

      verifyAll();
   }

   public void testExceptionClosingCatalogDoesNotPreventEnvironmentFromClosing() throws Exception {
      start();
      cacheDb.close();
      expiryDb.close();
      catalog.close();
      expectLastCall().andThrow(new DatabaseException("Dummy"){});
      env.close();
      replayAll();
      cs.start();
      cs.stop();
      verifyAll();
   }

   @Test(expectedExceptions = CacheLoaderException.class)
   public void testExceptionClosingEnvironment() throws Exception {
      start();
      cacheDb.close();
      expiryDb.close();
      catalog.close();
      env.close();
      expectLastCall().andThrow(new DatabaseException("Dummy"){});
      replayAll();
      cs.start();
      cs.stop();
      verifyAll();
   }


   @Test(expectedExceptions = CacheLoaderException.class)
   public void testThrowsCorrectExceptionOnStartForDatabaseException() throws Exception {
      factory = new MockBdbjeResourceFactory(cfg) {
         @Override
         public StoredClassCatalog createStoredClassCatalog(Database catalogDb) throws DatabaseException {
            throw new DatabaseException("Dummy"){};
         }
      };
      start();
      replayAll();
      cs.start();

   }

   @Test(expectedExceptions = CacheLoaderException.class)
   public void testEnvironmentDirectoryExistsButNotAFile() throws Exception {
      File file = createMock(File.class);
      expect(file.exists()).andReturn(true);
      expect(file.isDirectory()).andReturn(false);
      replay(file);
      cs.verifyOrCreateEnvironmentDirectory(file);
   }

   @Test(expectedExceptions = CacheLoaderException.class)
   public void testCantCreateEnvironmentDirectory() throws Exception {
      File file = createMock(File.class);
      expect(file.exists()).andReturn(false);
      expect(file.mkdirs()).andReturn(false);
      replay(file);
      cs.verifyOrCreateEnvironmentDirectory(file);
   }

   public void testCanCreateEnvironmentDirectory() throws Exception {
      File file = createMock(File.class);
      expect(file.exists()).andReturn(false);
      expect(file.mkdirs()).andReturn(true);
      expect(file.isDirectory()).andReturn(true);
      replay(file);
      assert file.equals(cs.verifyOrCreateEnvironmentDirectory(file));
   }

   public void testNoExceptionOnRollback() throws Exception {
      start();
      GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
      replayAll();
      cs.start();
      cs.rollback(tx);
      verifyAll();
   }

   public  void testApplyModificationsThrowsOriginalDatabaseException() throws Exception {
      start();
      DatabaseException ex = new DatabaseException("Dummy"){};
      runner.run(isA(TransactionWorker.class));
      expectLastCall().andThrow(new RuntimeExceptionWrapper(ex));
      replayAll();
      cs.start();
      try {
         cs.applyModifications(Collections.singletonList(new Store(TestInternalCacheEntryFactory.create("k", "v"))));
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         verifyAll();
         return;
      }
      assert false : "should have returned";

   }

   public void testCommitThrowsOriginalDatabaseException() throws Exception {
      start();
      DatabaseException ex = new DatabaseException("Dummy"){};
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
         GlobalTransaction t = gtf.newGlobalTransaction(null, false);
         cs.prepare(Collections.singletonList(new Store(TestInternalCacheEntryFactory.create("k", "v"))), t, false);
         cs.commit(t);
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         verifyAll();
         return;
      }
      assert false : "should have returned";

   }

   public void testPrepareThrowsOriginalDatabaseException() throws Exception {
      start();
      DatabaseException ex = new DatabaseException("Dummy"){};
      runner.prepare(isA(TransactionWorker.class));
      expectLastCall().andThrow(new RuntimeExceptionWrapper(ex));
      replayAll();
      cs.start();
      try {
         GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
         cs.prepare(Collections.singletonList(new Store(TestInternalCacheEntryFactory.create("k", "v"))), tx, false);
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         verifyAll();
         return;
      }
      assert false : "should have returned";

   }

   public void testClearOnAbortFromStream() throws Exception {
      start();
      InternalCacheEntry entry = TestInternalCacheEntryFactory.create("key", "value");
      expect(cacheMap.put(entry.getKey(), entry)).andReturn(null);
      ObjectInput ois = createMock(ObjectInput.class);
      expect(ois.readLong()).andReturn((long) 1);
      com.sleepycat.je.Transaction txn = createMock(com.sleepycat.je.Transaction.class);
      expect(currentTransaction.beginTransaction(null)).andReturn(txn);
      Cursor cursor = createMock(Cursor.class);
      expect(cacheDb.openCursor(txn, null)).andReturn(cursor);
      IOException ex = new IOException();
      expect(ois.readObject()).andReturn(new byte[0]);
      expectLastCall().andThrow(ex);
      txn.abort();
      cacheMap.clear();
      expiryMap.clear();
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
