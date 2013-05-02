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
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.util.ExceptionUnwrapper;
import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.bdbje.logging.Log;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An Oracle SleepyCat JE implementation of a {@link org.infinispan.loaders.CacheStore}.  <p/>This implementation uses
 * two databases <ol> <li>stored entry database: <tt>/{location}/CacheInstance-{@link org.infinispan.Cache#getName()
 * name}</tt></li> {@link org.infinispan.container.entries.InternalCacheEntry stored entries} are stored here, keyed on
 * {@link org.infinispan.container.entries.InternalCacheEntry#getKey()} <li>class catalog database:
 * <tt>/{location}/CacheInstance-{@link org.infinispan.Cache#getName() name}_class_catalog</tt></li> class descriptions
 * are stored here for efficiency reasons. </ol> <p/> <p><tt>/{location}/je.properties</tt> is optional and will
 * override any parameters to the internal SleepyCat JE {@link EnvironmentConfig}.</p>
 * <p/>
 * All data access is transactional.  Any attempted reads to locked records will block.  The maximum duration of this is
 * set in nanoseconds via the parameter {@link org.infinispan.loaders.bdbje.BdbjeCacheStoreConfig#getLockAcquistionTimeout()}.
 * Calls to {@link org.infinispan.loaders.CacheStore#prepare(java.util.List, org.infinispan.transaction.xa.GlobalTransaction,
 * boolean)}  will attempt to resolve deadlocks, retrying up to {@link org.infinispan.loaders.bdbje.BdbjeCacheStoreConfig#getMaxTxRetries()}
 * attempts.
 * <p/>
 * Unlike the C version of SleepyCat, JE does not support MVCC or READ_COMMITTED isolation.  In other words, readers
 * will block on any data held by a pending transaction.  As such, it is best practice to keep the duration between
 * <code>prepare</code> and <code>commit</code> as short as possible.
 * <p/>
 *
 * @author Adrian Cole
 * @author Manik Surtani
 * @since 4.0
 */
@CacheLoaderMetadata(configurationClass = BdbjeCacheStoreConfig.class)
public class BdbjeCacheStore extends AbstractCacheStore {

   private static final Log log =
         LogFactory.getLog(BdbjeCacheStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private BdbjeCacheStoreConfig cfg;

   private Environment env;
   private StoredClassCatalog catalog;
   private Database cacheDb;
   private Database expiryDb;
   private StoredMap<Object, InternalCacheEntry> cacheMap;
   private StoredSortedMap<Long, Object> expiryMap;


   private PreparableTransactionRunner transactionRunner;
   private Map<GlobalTransaction, Transaction> txnMap;
   private CurrentTransaction currentTransaction;
   private BdbjeResourceFactory factory;

   /**
    * {@inheritDoc} This implementation expects config to be an instance of {@link BdbjeCacheStoreConfig} <p /> note
    * that the <code>m</code> is not currently used as SleepyCat has its own efficient solution.
    *
    * @see BdbjeCacheStoreConfig
    */
   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      BdbjeCacheStoreConfig cfg = (BdbjeCacheStoreConfig) config;
      init(cfg, new BdbjeResourceFactory(cfg), cache, m);
   }

   void init(BdbjeCacheStoreConfig cfg, BdbjeResourceFactory factory, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      if (trace) log.trace("initializing BdbjeCacheStore");
      printLicense();
      super.init(cfg, cache, m);
      this.cfg = cfg;
      this.factory = factory;
   }

   /**
    * {@inheritDoc}
    *
    * @return {@link BdbjeCacheStoreConfig}
    */
   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return org.infinispan.loaders.bdbje.BdbjeCacheStoreConfig.class;
   }

   /**
    * {@inheritDoc} Validates configuration, configures and opens the {@link Environment}, then {@link
    * org.infinispan.loaders.bdbje.BdbjeCacheStore#openSleepyCatResources() opens the databases}.  When this is
    * finished, transactional and purging services are instantiated.
    */
   @Override
   public void start() throws CacheLoaderException {
      if (trace) log.trace("starting BdbjeCacheStore");

      openSleepyCatResources();
      openTransactionServices();
      super.start();

      log.debugf("started cache store %s", this);
   }

   private void openTransactionServices() {
      txnMap = CollectionFactory.makeConcurrentMap(64, getConcurrencyLevel());
      currentTransaction = factory.createCurrentTransaction(env);
      transactionRunner = factory.createPreparableTransactionRunner(env);
   }

   /**
    * Opens the SleepyCat environment and all databases.  A {@link StoredMap} instance is provided which persists the
    * CacheStore.
    */
   private void openSleepyCatResources() throws CacheLoaderException {
      if (trace) log.tracef("creating je environment with home dir %s", cfg.getLocation());

      cfg.setCacheName(cache.getName());
      if (cfg.getCatalogDbName() == null)
         cfg.setCatalogDbName(cfg.getCacheDbName() + "_class_catalog");

      File location = verifyOrCreateEnvironmentDirectory(new File(cfg.getLocation()));
      try {
         env = factory.createEnvironment(location, cfg.readEnvironmentProperties());
         cacheDb = factory.createDatabase(env, cfg.getCacheDbName());
         Database catalogDb = factory.createDatabase(env, cfg.getCatalogDbName());
         expiryDb = factory.createDatabase(env, cfg.getExpiryDbName());
         catalog = factory.createStoredClassCatalog(catalogDb);
         cacheMap = factory.createStoredMapViewOfDatabase(cacheDb, catalog, marshaller);
         expiryMap = factory.createStoredSortedMapForKeyExpiry(expiryDb, catalog, marshaller);
      } catch (DatabaseException e) {
         throw convertToCacheLoaderException("could not open sleepycat je resource", e);
      }
   }

   // not private so that this can be unit tested

   File verifyOrCreateEnvironmentDirectory(File location) throws CacheLoaderException {
      if (!location.exists()) {
         boolean created = location.mkdirs();
         if (!created) throw new CacheLoaderException("Unable to create cache loader location " + location);

      }
      if (!location.isDirectory()) {
         throw new CacheLoaderException("Cache loader location [" + location + "] is not a directory!");
      }
      return location;
   }

   /**
    * Stops transaction and purge processing and closes the SleepyCat environment.  The environment and databases are
    * not removed from the file system. Exceptions during close of databases are ignored as closing the environment
    * will ensure the databases are also.
    */
   @Override
   public void stop() throws CacheLoaderException {
      if (trace) log.trace("stopping BdbjeCacheStore");
      super.stop();
      closeTransactionServices();
      closeSleepyCatResources();
      log.debugf("started cache store %s", this);
   }

   private void closeTransactionServices() {
      transactionRunner = null;
      currentTransaction = null;
      txnMap = null;
   }

   private void closeSleepyCatResources() throws CacheLoaderException {
      cacheMap = null;
      expiryMap = null;
      closeDatabases();
      closeEnvironment();
   }

   /**
    * Exceptions are ignored so that {@link org.infinispan.loaders.bdbje.BdbjeCacheStore#closeEnvironment()}  will
    * execute.
    */
   private void closeDatabases() {
      if (trace) log.trace("closing databases");
      try {
         cacheDb.close();
      } catch (Exception e) {
         log.errorClosingDatabase(e);
      }
      try {
         expiryDb.close();
      } catch (Exception e) {
         log.errorClosingDatabase(e);
      }
      try {
         catalog.close();
      } catch (Exception e) {
         log.errorClosingCatalog(e);
      }
      catalog = null;
      cacheDb = null;
      expiryDb = null;
   }

   private void closeEnvironment() throws CacheLoaderException {
      if (env != null) {
         try {
            env.close();
         } catch (DatabaseException e) {
            throw new CacheLoaderException("Unexpected exception closing cacheStore", e);
         }
      }
      env = null;
   }

   /**
    * {@inheritDoc} delegates to {@link BdbjeCacheStore#applyModifications(java.util.List)}, if
    * <code>isOnePhase</code>. Otherwise, delegates to {@link BdbjeCacheStore#prepare(java.util.List,
    * org.infinispan.transaction.xa.GlobalTransaction)}
    */
   @Override
   public void prepare(List<? extends Modification> mods, GlobalTransaction tx, boolean isOnePhase) throws CacheLoaderException {
      if (isOnePhase) {
         applyModifications(mods);
      } else {
         prepare(mods, tx);
      }
   }

   /**
    * Perform the <code>mods</code> atomically by creating a {@link ModificationsTransactionWorker worker} and invoking
    * them in {@link PreparableTransactionRunner#run(com.sleepycat.collections.TransactionWorker)}.
    *
    * @param mods actions to perform atomically
    * @throws CacheLoaderException on problems during the transaction
    */
   @Override
   protected void applyModifications(List<? extends Modification> mods) throws CacheLoaderException {
      if (trace) log.trace("performing one phase transaction");
      try {
         transactionRunner.run(new ModificationsTransactionWorker(this, mods));
      } catch (Exception caught) {
         throw convertToCacheLoaderException("Problem committing modifications: " + mods, caught);
      }
   }

   /**
    * Looks up the {@link Transaction SleepyCat transaction} associated with <code>tx</code>.  Creates a {@link
    * org.infinispan.loaders.bdbje.ModificationsTransactionWorker} instance from <code>mods</code>.  Then prepares the
    * transaction via {@link PreparableTransactionRunner#prepare(com.sleepycat.collections.TransactionWorker)}.
    * Finally, it invalidates {@link com.sleepycat.collections.CurrentTransaction#getTransaction()} so that no other
    * thread can accidentally commit this.
    *
    * @param mods modifications to be applied
    * @param tx   transaction identifier
    * @throws CacheLoaderException in the event of problems writing to the store
    */
   protected void prepare(List<? extends Modification> mods, GlobalTransaction tx) throws CacheLoaderException {
      if (trace) log.tracef("preparing transaction %s", tx);
      try {
         transactionRunner.prepare(new ModificationsTransactionWorker(this, mods));
         Transaction txn = currentTransaction.getTransaction();
         if (trace) log.tracef("transaction %s == sleepycat transaction %s", tx, txn);
         txnMap.put(tx, txn);
         ((ThreadLocal)ReflectionUtil.getValue(currentTransaction, "localTrans")).remove();
      } catch (Exception e) {
         throw convertToCacheLoaderException("Problem preparing transaction", e);
      }
   }


   /**
    * {@inheritDoc}
    * <p/>
    * This implementation calls {@link BdbjeCacheStore#completeTransaction(org.infinispan.transaction.xa.GlobalTransaction,
    * boolean)} completeTransaction} with an argument of false.
    */
   @Override
   public void rollback(GlobalTransaction tx) {
      try {
         completeTransaction(tx, false);
      } catch (Exception e) {
         log.rollingBackAfterError(e);
      }
   }

   /**
    * {@inheritDoc}
    * <p/>
    * This implementation calls {@link BdbjeCacheStore#completeTransaction(org.infinispan.transaction.xa.GlobalTransaction,
    * boolean)} completeTransaction} with an argument of true.
    */
   @Override
   public void commit(GlobalTransaction tx) throws CacheLoaderException {
      completeTransaction(tx, true);
   }

   /**
    * Looks up the SleepyCat transaction associated with the parameter <code>tx</code>.  If there is no associated
    * sleepycat transaction, an error is logged.
    *
    * @param tx     java transaction used to lookup a SleepyCat transaction
    * @param commit true to commit false to abort
    * @throws CacheLoaderException if there are problems committing or aborting the transaction
    */
   protected void completeTransaction(GlobalTransaction tx, boolean commit) throws CacheLoaderException {
      Transaction txn = txnMap.remove(tx);
      if (txn != null) {
         if (trace) log.tracef("%s sleepycat transaction %s", commit ? "committing" : "aborting", txn);
         try {
            if (commit)
               txn.commit();
            else
               txn.abort();
         } catch (Exception caught) {
            throw convertToCacheLoaderException("Problem completing transaction", caught);
         }
      } else {
         if (trace) log.tracef("no sleepycat transaction associated  transaction %s", tx);
      }
   }

   /**
    * commits or aborts the {@link com.sleepycat.collections.CurrentTransaction#getTransaction() current transaction}
    *
    * @param commit true to commit, false to abort
    * @throws CacheLoaderException if there was a problem completing the transaction
    */
   private void completeCurrentTransaction(boolean commit) throws CacheLoaderException {
      try {
         if (trace)
            log.tracef("%s current sleepycat transaction %s", commit ? "committing" : "aborting", currentTransaction.getTransaction());
         if (commit)
            currentTransaction.commitTransaction();
         else
            currentTransaction.abortTransaction();
      } catch (Exception caught) {
         throw convertToCacheLoaderException("Problem completing transaction", caught);
      }
   }

   /**
    * {@inheritDoc} This implementation delegates to {@link StoredMap#remove(Object)}
    */
   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      try {
         if (cacheMap.containsKey(key)) {
            cacheMap.remove(key);
            return true;
         }
         return false;
      } catch (RuntimeException caught) {
         throw convertToCacheLoaderException("error removing key " + key, caught);
      }
   }

   /**
    * {@inheritDoc} This implementation delegates to {@link StoredMap#get(Object)}.  If the object is expired, it will
    * not be returned.
    */
   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      try {
         InternalCacheEntry s = cacheMap.get(key);
         if (s != null && s.isExpired(timeService.wallClockTime())) {
            s = null;
         }
         return s;
      } catch (RuntimeException caught) {
         throw convertToCacheLoaderException("error loading key " + key, caught);
      }
   }

   /**
    * {@inheritDoc} This implementation delegates to {@link StoredMap#put(Object, Object)}
    */
   @Override
   public void store(InternalCacheEntry ed) throws CacheLoaderException {
      try {
         cacheMap.put(ed.getKey(), ed);
         if (ed.canExpire())
            addNewExpiry(ed);
      } catch (IOException caught) {
         throw convertToCacheLoaderException("error storing entry " + ed, caught);
      }
   }


   private void addNewExpiry(InternalCacheEntry entry) throws IOException {
      long expiry = entry.getExpiryTime();
      if (entry.getMaxIdle() > 0) {
         // Coding getExpiryTime() for transient entries has the risk of being a moving target
         // which could lead to unexpected results, hence, InternalCacheEntry calls are required
         expiry = entry.getMaxIdle() + timeService.wallClockTime();
      }
      Long at = expiry;
      Object key = entry.getKey();
      expiryMap.put(at, key);
   }

   /**
    * {@inheritDoc} This implementation delegates to {@link StoredMap#clear()}
    */
   @Override
   public void clear() throws CacheLoaderException {
      try {
         cacheMap.clear();
         expiryMap.clear();
      } catch (RuntimeException caught) {
         throw convertToCacheLoaderException("error clearing store", caught);
      }
   }

   /**
    * {@inheritDoc} This implementation returns a Set from {@link StoredMap#values()}
    */
   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      try {
         return new HashSet<InternalCacheEntry>(cacheMap.values());
      } catch (RuntimeException caught) {
         throw convertToCacheLoaderException("error loading all entries", caught);
      }
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      if (numEntries < 0) return loadAll();
      try {
         Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>(numEntries);
         for (Iterator<InternalCacheEntry> i = cacheMap.values().iterator(); i.hasNext() && s.size() < numEntries;)
            s.add(i.next());
         return s;
      } catch (RuntimeException caught) {
         throw convertToCacheLoaderException("error loading all entries", caught);
      }
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      try {
         Set<Object> s = new HashSet<Object>();
         for (Object o: cacheMap.keySet()) if (keysToExclude == null || !keysToExclude.contains(o)) s.add(o);
         return s;
      } catch (RuntimeException caught) {
         throw convertToCacheLoaderException("error loading all entries", caught);
      }
   }

   /**
    * {@inheritDoc} This implementation reads the number of entries to load from the stream, then begins a transaction.
    * During that transaction, the cachestore is cleared and replaced with entries from the stream.  If there are any
    * errors during the process, the entire transaction is rolled back.  Deadlock handling is not addressed, as there
    * is no means to rollback reads from the input stream.
    *
    * @see BdbjeCacheStore#toStream(java.io.ObjectOutput)
    */
   @Override
   public void fromStream(ObjectInput ois) throws CacheLoaderException {
      try {
         currentTransaction.beginTransaction(null);
         for (Database db : new Database[]{cacheDb, expiryDb}) {
            long recordCount = ois.readLong();
            log.debugf("clearing and reading %s records from stream", recordCount);
            Cursor cursor = null;
            try {
               cursor = db.openCursor(currentTransaction.getTransaction(), null);
               for (int i = 0; i < recordCount; i++) {
                  byte[] keyBytes = (byte[]) ois.readObject();
                  byte[] dataBytes = (byte[]) ois.readObject();

                  DatabaseEntry key = new DatabaseEntry(keyBytes);
                  DatabaseEntry data = new DatabaseEntry(dataBytes);
                  cursor.put(key, data);
               }
            } finally {
               if (cursor != null) cursor.close();
            }
         }
         completeCurrentTransaction(true);
      } catch (Exception caught) {
         completeCurrentTransaction(false);
         clear();
         throw convertToCacheLoaderException("Problems reading from stream", caught);
      }
   }

   /**
    * Writes the current count of cachestore entries followed by a pair of byte[]s corresponding to the SleepyCat
    * binary representation of {@link InternalCacheEntry#getKey() key} {@link InternalCacheEntry value}.
    * <p/>
    * This implementation holds a transaction open to ensure that we see no new records added while iterating.
    */
   @Override
   public void toStream(ObjectOutput oos) throws CacheLoaderException {
      try {
         currentTransaction.beginTransaction(null);
         for (Database db : new Database[]{cacheDb, expiryDb}) {
            long recordCount = db.count();
            oos.writeLong(recordCount);
            if (trace) log.tracef("writing %s records to stream", recordCount);
            Cursor cursor = null;
            try {
               cursor = db.openCursor(currentTransaction.getTransaction(), null);
               DatabaseEntry key = new DatabaseEntry();
               DatabaseEntry data = new DatabaseEntry();
               int recordsWritten = 0;
               while (cursor.getNext(key, data, null) ==
                     OperationStatus.SUCCESS) {
                  oos.writeObject(key.getData());
                  oos.writeObject(data.getData());
                  recordsWritten++;
               }
               if (trace) log.tracef("wrote %s records to stream", recordsWritten);
               if (recordsWritten != recordCount)
                  log.unexpectedNumberRecordsWritten(recordCount, recordsWritten);
            } finally {
               if (cursor != null) cursor.close();
            }
         }
         completeCurrentTransaction(true);
      } catch (Exception caught) {
         completeCurrentTransaction(false);
         throw convertToCacheLoaderException("Problems writing to stream", caught);
      }
   }

   /**
    * In order to adhere to APIs which do not throw checked exceptions, BDBJE wraps IO and DatabaseExceptions inside
    * RuntimeExceptions.  These special Exceptions implement {@link com.sleepycat.util.ExceptionWrapper}.  This method
    * will look for any of that type of Exception and encapsulate it into a CacheLoaderException.  In doing so, the
    * real root cause can be obtained.
    *
    * @param message what to attach to the CacheLoaderException
    * @param caught  exception to parse
    * @return CacheLoaderException with the correct cause
    */
   CacheLoaderException convertToCacheLoaderException(String message, Exception caught) {
      caught = ExceptionUnwrapper.unwrap(caught);
      return (caught instanceof CacheLoaderException) ? (CacheLoaderException) caught :
            new CacheLoaderException(message, caught);
   }

   /**
    * Iterate through {@link com.sleepycat.collections.StoredMap#entrySet()} and remove, if expired.
    */
   @Override
   protected void purgeInternal() throws CacheLoaderException {
      try {
         Map<Long, Object> expired = expiryMap.headMap(timeService.wallClockTime(), true);
         for (Map.Entry<Long, Object> entry : expired.entrySet()) {
            expiryMap.remove(entry.getKey());
            cacheMap.remove(entry.getValue());
         }
      } catch (RuntimeException caught) {
         throw convertToCacheLoaderException("error purging expired entries", caught);
      }
   }

   /**
    * prints terms of use for Berkeley DB JE
    */
   public void printLicense() {
      String license = "\n*************************************************************************************\n" +
            "Berkeley DB Java Edition version: " + JEVersion.CURRENT_VERSION.toString() + "\n" +
            "Infinispan can use Berkeley DB Java Edition from Oracle \n" +
            "(http://www.oracle.com/database/berkeley-db/je/index.html)\n" +
            "for persistent, reliable and transaction-protected data storage.\n" +
            "If you choose to use Berkeley DB Java Edition with Infinispan, you must comply with the terms\n" +
            "of Oracle's public license, included in the file LICENSE.txt.\n" +
            "If you prefer not to release the source code for your own application in order to comply\n" +
            "with the Oracle public license, you may purchase a different license for use of\n" +
            "Berkeley DB Java Edition with Infinispan.\n" +
            "See http://www.oracle.com/database/berkeley-db/je/index.html for pricing and license terms\n" +
            "*************************************************************************************";
      System.out.println(license);
   }

}
