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

import static org.mockito.Mockito.*;

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
      cache = mock(Cache.class);
      cs = new BdbjeCacheStore();
      env = mock(Environment.class);
      cacheDb = mock(Database.class);
      catalogDb = mock(Database.class);
      expiryDb = mock(Database.class);
      catalog = mock(StoredClassCatalog.class);
      cacheMap = mock(StoredMap.class);
      expiryMap = mock(StoredSortedMap.class);
      currentTransaction = mock(CurrentTransaction.class);
      gtf = new TransactionFactory();
      gtf.init(false, false, true, false);
      WeakReference<Environment> envRef = new WeakReference<Environment>(env);
      ReflectionUtil.setValue(currentTransaction, "envRef", envRef);
      ThreadLocal localTrans = new ThreadLocal();
      ReflectionUtil.setValue(currentTransaction, "localTrans", localTrans);
      runner = mock(PreparableTransactionRunner.class);
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
      when(cache.getName()).thenReturn("cache");
      when(cache.getConfiguration()).thenReturn(null);
   }

   public void testGetConfigurationClass() throws Exception {
      assert cs.getConfigurationClass().equals(BdbjeCacheStoreConfig.class);
   }

   public void testInitNoMock() throws Exception {
      cs.init(cfg, cache, null);
      assert cfg.equals(ReflectionUtil.getValue(cs, "cfg"));
      assert cache.equals(ReflectionUtil.getValue(cs, "cache"));
      assert ReflectionUtil.getValue(cs, "factory") instanceof BdbjeResourceFactory;
   }

   public void testExceptionClosingCacheDatabaseDoesNotPreventEnvironmentFromClosing() throws Exception {
      start();
      doThrow(new DatabaseException("Dummy") {}).when(expiryDb).close();
      cs.start();
      cs.stop();
   }

   public void testExceptionClosingCatalogDoesNotPreventEnvironmentFromClosing() throws Exception {
      start();
      doThrow(new DatabaseException("Dummy") {}).when(catalog).close();
      cs.start();
      cs.stop();
   }

   @Test(expectedExceptions = CacheLoaderException.class)
   public void testExceptionClosingEnvironment() throws Exception {
      start();
      doThrow(new DatabaseException("Dummy") {}).when(env).close();
      cs.start();
      cs.stop();
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
      cs.start();

   }

   @Test(expectedExceptions = CacheLoaderException.class)
   public void testEnvironmentDirectoryExistsButNotAFile() throws Exception {
      File file = mock(File.class);
      when(file.exists()).thenReturn(true);
      when(file.isDirectory()).thenReturn(false);
      cs.verifyOrCreateEnvironmentDirectory(file);
   }

   @Test(expectedExceptions = CacheLoaderException.class)
   public void testCantCreateEnvironmentDirectory() throws Exception {
      File file = mock(File.class);
      when(file.exists()).thenReturn(false);
      when(file.mkdirs()).thenReturn(false);
      cs.verifyOrCreateEnvironmentDirectory(file);
   }

   public void testCanCreateEnvironmentDirectory() throws Exception {
      File file = mock(File.class);
      when(file.exists()).thenReturn(false);
      when(file.mkdirs()).thenReturn(true);
      when(file.isDirectory()).thenReturn(true);
      assert file.equals(cs.verifyOrCreateEnvironmentDirectory(file));
   }

   public void testNoExceptionOnRollback() throws Exception {
      start();
      GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
      cs.start();
      cs.rollback(tx);
   }

   public  void testApplyModificationsThrowsOriginalDatabaseException() throws Exception {
      start();
      DatabaseException ex = new DatabaseException("Dummy"){};
      doThrow(new RuntimeExceptionWrapper(ex)).when(runner).run(isA(TransactionWorker.class));
      cs.start();
      try {
         cs.applyModifications(Collections.singletonList(new Store(TestInternalCacheEntryFactory.create("k", "v"))));
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         return;
      }
      assert false : "should have returned";

   }

   public void testCommitThrowsOriginalDatabaseException() throws Exception {
      start();
      DatabaseException ex = new DatabaseException("Dummy"){};
      com.sleepycat.je.Transaction txn = mock(com.sleepycat.je.Transaction.class);
      when(currentTransaction.beginTransaction(null)).thenReturn(txn);
      runner.prepare(isA(TransactionWorker.class));
      doThrow(new RuntimeExceptionWrapper(ex)).when(txn).commit();
      cs.start();
      try {
         txn = currentTransaction.beginTransaction(null);
         GlobalTransaction t = gtf.newGlobalTransaction(null, false);
         cs.prepare(Collections.singletonList(new Store(TestInternalCacheEntryFactory.create("k", "v"))), t, false);
         cs.commit(t);
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         return;
      }
      assert false : "should have returned";

   }

   public void testPrepareThrowsOriginalDatabaseException() throws Exception {
      start();
      DatabaseException ex = new DatabaseException("Dummy"){};
      doThrow(new RuntimeExceptionWrapper(ex)).when(runner).prepare(isA(TransactionWorker.class));
      cs.start();
      try {
         GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
         cs.prepare(Collections.singletonList(new Store(TestInternalCacheEntryFactory.create("k", "v"))), tx, false);
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         return;
      }
      assert false : "should have returned";

   }

   public void testClearOnAbortFromStream() throws Exception {
      start();
      InternalCacheEntry entry = TestInternalCacheEntryFactory.create("key", "value");
      when(cacheMap.put(entry.getKey(), entry)).thenReturn(null);
      ObjectInput ois = mock(ObjectInput.class);
      when(ois.readLong()).thenReturn((long) 1);
      com.sleepycat.je.Transaction txn = mock(com.sleepycat.je.Transaction.class);
      when(currentTransaction.beginTransaction(null)).thenReturn(txn);
      Cursor cursor = mock(Cursor.class);
      when(cacheDb.openCursor(txn, null)).thenReturn(cursor);
      IOException ex = new IOException();
      when(ois.readObject()).thenThrow(ex);
      cacheMap.clear();
      expiryMap.clear();
      cs.start();
      try {
         cs.store(entry);
         cs.fromStream(ois);
         assert false : "should have gotten an exception";
      } catch (CacheLoaderException e) {
         assert ex.equals(e.getCause());
         return;
      }
      assert false : "should have returned";
   }
}
