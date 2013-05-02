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
package org.infinispan.loaders.jdbm;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.FastIterator;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import jdbm.htree.HTree;
import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.loaders.jdbm.logging.Log;
import org.infinispan.util.SysPropertyActions;
import org.infinispan.util.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.text.SimpleDateFormat;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A persistent <code>CacheLoader</code> based on the JDBM project. See http://jdbm.sourceforge.net/ . Does not support
 * transaction isolation.
 * <p/>
 * Supports removal of expired entries.
 * <p/>
 * It would probably be better if meta-data (expiry time) was stored independent of the value of the entry. That is, if
 * (key,"m") == meta and (key,"v") == value.
 *
 * @author Elias Ross
 * @author Galder Zamarreño
 */
@ThreadSafe
@CacheLoaderMetadata(configurationClass = JdbmCacheStoreConfig.class)
public class JdbmCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(JdbmCacheStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final String NAME = "CacheLoader";
   private static final String EXPIRY = "Expiry";
   private static final String DATE = "HH:mm:ss.SSS";

   private BlockingQueue<ExpiryEntry> expiryEntryQueue;

   private JdbmCacheStoreConfig config;
   private RecordManager recman;
   private HTree tree;
   private BTree expiryTree;

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return JdbmCacheStoreConfig.class;
   }

   @Override
   public void init(CacheLoaderConfig clc, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(clc, cache, m);
      this.config = (JdbmCacheStoreConfig) clc;
   }

   @Override
   public void start() throws CacheLoaderException {
      String locationStr = config.getLocation();
      if (locationStr == null) {
         locationStr = SysPropertyActions.getProperty("java.io.tmpdir");
         config.setLocation(locationStr);
      }

      expiryEntryQueue = new LinkedBlockingQueue<ExpiryEntry>(config.getExpiryQueueSize());

      // JBCACHE-1448 db name parsing fix courtesy of Ciro Cavani
      /* Parse config string. */
      int offset = locationStr.indexOf('#');
      String cacheDbName;
      if (offset >= 0 && offset < locationStr.length() - 1) {
         cacheDbName = locationStr.substring(offset + 1);
         locationStr = locationStr.substring(0, offset);
      } else {
         cacheDbName = cache.getName();
         if (cacheDbName == null)
            cacheDbName = "jdbm";
      }

      // test location
      File location = new File(locationStr);
      if (!location.exists()) {
         boolean created = location.mkdirs();
         if (!created)
            throw new ConfigurationException("Unable to create cache loader location " + location);
      }
      if (!location.isDirectory()) {
         throw new ConfigurationException("Cache loader location [" + location + "] is not a directory!");
      }

      try {
         openDatabase(new File(location, cacheDbName));
      } catch (Exception e) {
         throw new ConfigurationException(e);
      }

      log.debug("cleaning up expired entries...");
      purgeInternal();

      log.debug("started");
      super.start();
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      try {
         InternalCacheEntry ice = unmarshall(tree.get(key), key);
         if (ice != null && ice.isExpired(timeService.wallClockTime())) {
            remove(key);
            return null;
         }
         return ice;
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return new BTreeSet();
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      return new BTreeSet(numEntries);
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      try {
         Set<Object> s = new HashSet<Object>();
         FastIterator fi = tree.keys();
         Object o;
         while ((o = fi.next()) != null) if (keysToExclude == null || !keysToExclude.contains(o)) s.add(o);
         return s;
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Opens all databases and initializes database related information.
    */
   private void openDatabase(File f) throws Exception {
      Properties props = new Properties();
      // Incorporate properties from setConfig() ?
      // props.put(RecordManagerOptions.SERIALIZER,
      // RecordManagerOptions.SERIALIZER_EXTENSIBLE);
      // props.put(RecordManagerOptions.PROFILE_SERIALIZATION, "false");
      recman = RecordManagerFactory.createRecordManager(f.toString(), props);
      long recid = recman.getNamedObject(NAME);
      log.debugf("%s located as %d", NAME, recid);
      if (recid == 0) {
         createTree();
      } else {
         tree = HTree.load(recman, recid);
         recid = recman.getNamedObject(EXPIRY);
         expiryTree = BTree.load(recman, recid);
         setSerializer();
      }

      log.jdbmDbOpened(f);
   }

   /**
    * Resets the value serializer to point to our marshaller.
    */
   private void setSerializer() {
      // TODO explore how to use our marshaller with HTree
      // tree.setValueSerializer(new JdbmSerializer(getMarshaller()));
      expiryTree.setValueSerializer(new JdbmSerializer(getMarshaller()));
   }

   private void createTree() throws IOException {
      tree = HTree.createInstance(recman);
      expiryTree = BTree.createInstance(recman, new NaturalComparator(), null, null);
      recman.setNamedObject(NAME, tree.getRecid());
      recman.setNamedObject(EXPIRY, expiryTree.getRecid());
      setSerializer();
   }

   /**
    * Closes all databases, ignoring exceptions, and nulls references to all database related information.
    */
   @Override
   public void stop() throws CacheLoaderException {
      super.stop();

      if (recman != null) {
         try {
            recman.close();
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }
      recman = null;
      tree = null;
      expiryTree = null;
   }

   @Override
   public void clear() throws CacheLoaderException {
      if (trace)
         log.trace("clear()");
      try {
         recman.delete(tree.getRecid());
         recman.delete(expiryTree.getRecid());
         createTree();
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      try {
         return remove0(key);
      } finally {
         commit();
      }
   }

   private void commit() throws CacheLoaderException {
      try {
         recman.commit();
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   public boolean remove0(Object key) throws CacheLoaderException {
      if (trace)
         log.tracef("remove() %s", key);
      try {
         // Not the most efficient way but JDBM offers no other API
         boolean ret = tree.get(key) != null;
         tree.remove(key);
         // If the key does not exist, HTree ignores the operation, so always return true
         return ret;
      } catch (IOException e) {
         // can happen during normal operation
         return false;
      }
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      store0(entry);
      commit();
   }

   private byte[] marshall(InternalCacheEntry entry) throws IOException, InterruptedException {
      return getMarshaller().objectToByteBuffer(entry.toInternalCacheValue());
   }

   private InternalCacheEntry unmarshall(Object o, Object key) throws IOException, ClassNotFoundException {
      if (o == null)
         return null;
      byte b[] = (byte[]) o;
      InternalCacheValue v = (InternalCacheValue) getMarshaller().objectFromByteBuffer(b);
      return v.toInternalCacheEntry(key);
   }

   private void store0(InternalCacheEntry entry) throws CacheLoaderException {
      Object key = entry.getKey();
      if (trace)
         log.tracef("store() %s", key);
      try {
         tree.put(key, marshall(entry));
         if (entry.canExpire())
            addNewExpiry(entry);
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      } catch (InterruptedException ie) {
         if (trace) log.trace("Interrupted while marshalling entry");
         Thread.currentThread().interrupt();
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
      if (trace) log.tracef("at %s expire %s", new SimpleDateFormat(DATE).format(new Date(at)), key);

      try {
         expiryEntryQueue.put(new ExpiryEntry(at, key));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt(); // Restore interruption status
      }
   }

   /**
    * Writes to a stream the number of entries (long) then the entries themselves.
    */
   @Override
   public void toStream(ObjectOutput out) throws CacheLoaderException {
      try {
         Set<InternalCacheEntry> loadAll = loadAll();
         log.debug("toStream() entries");
         int count = 0;
         for (InternalCacheEntry entry : loadAll) {
            getMarshaller().objectToObjectStream(entry, out);
            count++;
         }
         getMarshaller().objectToObjectStream(null, out);
         log.debugf("wrote %d entries", count);
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Reads from a stream the number of entries (long) then the entries themselves.
    */
   @Override
   public void fromStream(ObjectInput in) throws CacheLoaderException {
      try {
         log.debug("fromStream()");
         int count = 0;
         while (true) {
            count++;
            InternalCacheEntry entry = (InternalCacheEntry) getMarshaller().objectFromObjectStream(in);
            if (entry == null)
               break;
            store(entry);
         }
         log.debugf("read %d entries", count);
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheLoaderException(e);
      } catch (InterruptedException ie) {
         if (log.isTraceEnabled()) log.trace("Interrupted while reading from stream"); 
         Thread.currentThread().interrupt();
      }
   }

   /**
    * Purge expired entries.
    */
   @Override
   protected void purgeInternal() throws CacheLoaderException {
      log.trace("purgeInternal");
      try {
         purgeInternal0();
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Find all times less than current time. Build a list of keys for those times. Then purge those keys, assuming those
    * keys' expiry has not changed.
    *
    * @throws ClassNotFoundException
    */
   private void purgeInternal0() throws Exception {
      // Drain queue and update expiry tree
      List<ExpiryEntry> entries = new ArrayList<ExpiryEntry>();
      expiryEntryQueue.drainTo(entries);
      for (ExpiryEntry entry : entries) {
         Object existing = expiryTree.insert(entry.expiry, entry.key, false);
         if (existing != null) {
            // in the case of collision make the key a List ...
            if (existing instanceof List) {
               ((List<Object>) existing).add(entry.key);
               expiryTree.insert(entry.expiry, existing, true);
            } else {
               List<Object> al = new ArrayList<Object>(2);
               al.add(existing);
               al.add(entry.key);
               expiryTree.insert(entry.expiry, al, true);
            }
         }
      }

      // Browse the expiry and remove accordingly
      TupleBrowser browse = expiryTree.browse();
      Tuple tuple = new Tuple();
      List<Long> times = new ArrayList<Long>();
      List<Object> keys = new ArrayList<Object>();
      while (browse.getNext(tuple)) {
         Long time = (Long) tuple.getKey();
         if (time > timeService.wallClockTime())
            break;
         times.add(time);
         Object key = tuple.getValue();
         if (key instanceof List)
            keys.addAll((List<?>) key);
         else
            keys.add(key);
      }
      for (Long time : times) {
         expiryTree.remove(time);
      }

      if (!keys.isEmpty())
         log.debugf("purge (up to) %d entries", keys.size());
      int count = 0;
      long currentTimeMillis = timeService.wallClockTime();
      for (Object key : keys) {
         byte[] b = (byte[]) tree.get(key);
         if (b == null)
            continue;
         InternalCacheValue ice = (InternalCacheValue) getMarshaller().objectFromByteBuffer(b);
         if (ice.isExpired(currentTimeMillis)) {
            // somewhat inefficient to FIND then REMOVE...
            tree.remove(key);
            count++;
         }
      }
      if (count != 0)
         log.debugf("purged %d entries", count);
      recman.commit();
   }

   @Override
   protected void applyModifications(List<? extends Modification> mods) throws CacheLoaderException {
      for (Modification m : mods) {
         switch (m.getType()) {
            case STORE:
               store0(((Store) m).getStoredEntry());
               break;
            case CLEAR:
               clear();
               break;
            case REMOVE:
               remove0(((Remove) m).getKey());
               break;
            default:
               throw new AssertionError();
         }
      }
      commit();
   }

   @Override
   public String toString() {
      BTree et = expiryTree;
      int expiry = (et == null) ? -1 : et.size();
      return "JdbmCacheLoader locationStr=" + config.getLocation() + " expirySize=" + expiry;
   }

   private final class BTreeSet extends AbstractSet<InternalCacheEntry> {

      int maxSize = -1;

      private BTreeSet(int maxSize) {
         this.maxSize = maxSize;
      }

      private BTreeSet() {
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         final FastIterator fi;
         try {
            fi = tree.keys();
         } catch (IOException e) {
            throw new CacheException(e);
         }

         return new Iterator<InternalCacheEntry>() {
            int entriesReturned = 0;
            InternalCacheEntry current = null;
            boolean next = true;

            @Override
            public boolean hasNext() {
               if (current == null && next) {
                  Object key = fi.next();
                  if (key == null) {
                     next = false;
                  } else {
                     try {
                        current = unmarshall(tree.get(key), key);
                     } catch (IOException e) {
                        throw new CacheException(e);
                     } catch (ClassNotFoundException e) {
                        throw new CacheException(e);
                     }
                  }
               }
               if (next == true && entriesReturned >= maxSize && maxSize > -1) next = false;
               return next;
            }

            @Override
            public InternalCacheEntry next() {
               if (!hasNext())
                  throw new NoSuchElementException();
               try {
                  entriesReturned++;
                  return current;
               } finally {
                  current = null;
               }
            }

            @Override
            public void remove() {
               throw new UnsupportedOperationException();
            }

         };
      }

      @Override
      @SuppressWarnings("unused")
      public int size() {
         log.warn("size() should never be called; except for tests");
         int size = 0;
         for (Object dummy : this)
            size++;
         return size;
      }
   }

   private static final class ExpiryEntry {
      private final Long expiry;
      private final Object key;

      private ExpiryEntry(long expiry, Object key) {
         this.expiry = expiry;
         this.key = key;
      }
   }
}
