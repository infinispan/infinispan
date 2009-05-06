package org.infinispan.loaders.jdbm;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.text.SimpleDateFormat;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.Serializer;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import net.jcip.annotations.ThreadSafe;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A persistent <code>CacheLoader</code> based on the JDBM project. See
 * http://jdbm.sourceforge.net/ . Does not support transaction isolation.
 * <p/>
 * Supports removal of expired entries.
 * <p/>
 * It would probably be better if meta-data (expiry time) was stored independent
 * of the value of the entry. That is, if (key,"m") == meta and (key,"v") ==
 * value.
 * 
 * @author Elias Ross
 * @version $Id: JdbmCacheLoader.java 7261 2008-12-07 18:53:38Z genman $
 */
@ThreadSafe
public class JdbmCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(JdbmCacheStore.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final String NAME = "CacheLoader";
   private static final String EXPIRY = "Expiry";
   private final String DATE = "HH:mm:ss.SSS";

   private final Object expiryLock = new Object();

   private JdbmCacheStoreConfig config;
   private RecordManager recman;
   private BTree tree;
   private BTree expiryTree;
   private Cache cache;

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return JdbmCacheStoreConfig.class;
   }

   @Override
   public void init(CacheLoaderConfig clc, Cache cache, Marshaller m) {
      super.init(clc, cache, m);
      this.config = (JdbmCacheStoreConfig) clc;
      this.cache = cache;
   }

   @Override
   public void start() throws CacheLoaderException {
      String locationStr = config.getLocation();
      if (locationStr == null) {
         locationStr = System.getProperty("java.io.tmpdir");
         config.setLocation(locationStr);
      }

      // JBCACHE-1448 db name parsing fix courtesy of Ciro Cavani
      /* Parse config string. */
      int offset = locationStr.indexOf('#');
      String cacheDbName;
      if (offset >= 0 && offset < locationStr.length() - 1) {
         cacheDbName = locationStr.substring(offset + 1);
         locationStr = locationStr.substring(0, offset);
      } else {
         cacheDbName = cache.getName(); // TODO
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

   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      try {
         return (InternalCacheEntry) tree.find(key);
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return new BTreeSet();
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
      log.debug(NAME + " located as " + recid);
      if (recid == 0) {
         createTree();
      } else {
         tree = BTree.load(recman, recid);
         recid = recman.getNamedObject(EXPIRY);
         expiryTree = BTree.load(recman, recid);
         setSerializer();
      }

      log.info("JDBM database " + f + " opened with " + tree.size() + " entries");
   }

   /**
    * Resets the value serializer to point to our marshaller.
    */
   private void setSerializer() {
      tree.setValueSerializer(new JdbmSerializer(getMarshaller()));
      expiryTree.setValueSerializer(new JdbmSerializer(getMarshaller()));
   }

   private void createTree() throws IOException {
      tree = BTree.createInstance(recman, config.createComparator(), (Serializer) null, (Serializer) null);
      expiryTree = BTree.createInstance(recman, new NaturalComparator(), (Serializer) null, (Serializer) null);
      recman.setNamedObject(NAME, tree.getRecid());
      recman.setNamedObject(EXPIRY, expiryTree.getRecid());
      setSerializer();
   }

   /**
    * Closes all databases, ignoring exceptions, and nulls references to all
    * database related information.
    */
   @Override
   public void stop() {
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
         log.trace("remove() " + key);
      try {
         return tree.remove(key) != null;
      } catch (IllegalArgumentException e) {
         // can happen during normal operation
         return false;
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      store0(entry);
      commit();
   }

   private void store0(InternalCacheEntry entry) throws CacheLoaderException {
      Object key = entry.getKey();
      if (trace)
         log.trace("store() " + key);
      try {
         tree.insert(key, entry, true); // TODO a waste to ignore the return
                                        // value
         if (entry.canExpire())
            addNewExpiry(entry);
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   private void addNewExpiry(InternalCacheEntry entry) throws IOException {
      long expiry = entry.getExpiryTime();
      if (entry.getMaxIdle() > 0) {
         // TODO do we need both?
         expiry = entry.getMaxIdle() + System.currentTimeMillis();
      }
      Long at = new Long(expiry);
      Object key = entry.getKey();
      if (trace)
         log.trace("at " + new SimpleDateFormat(DATE).format(new Date(at)) + " expire " + key);
      // TODO could store expiry entries in a separate thread; would remove this
      // lock as well
      synchronized (expiryLock) {
         Object existing = expiryTree.insert(at, key, false);
         if (existing != null) {
            // in the case of collision make the key a List ...
            if (existing instanceof List) {
               ((List) existing).add(key);
               expiryTree.insert(at, existing, true);
            } else {
               List<Object> al = new ArrayList<Object>(2);
               al.add(existing);
               al.add(key);
               expiryTree.insert(at, al, true);
            }
         }
      }
   }

   /**
    * Writes to a stream the number of entries (long) then the entries
    * themselves.
    */
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      try {
         Set<InternalCacheEntry> loadAll = loadAll();
         outputStream.writeLong(loadAll.size());
         log.debug("toStream() " + loadAll.size() + " entries");
         for (InternalCacheEntry entry : loadAll)
            outputStream.writeObject(entry);
         log.debug("done!");
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Reads from a stream the number of entries (long) then the entries
    * themselves.
    */
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      try {
         long count = inputStream.readLong();
         log.debug("fromStream() " + count + " entries");
         for (int i = 0; i < count; i++) {
            InternalCacheEntry entry = (InternalCacheEntry) inputStream.readObject();
            store(entry);
         }
         log.debug("done!");
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheLoaderException(e);
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
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Find all times less than current time. Build a list of keys for those
    * times. Then purge those keys, assuming those keys' expiry has not changed.
    */
   private void purgeInternal0() throws IOException {
      TupleBrowser browse = expiryTree.browse();
      Tuple tuple = new Tuple();
      List<Long> times = new ArrayList<Long>();
      List<Object> keys = new ArrayList<Object>();
      synchronized (expiryLock) {
         while (browse.getNext(tuple)) {
            Long time = (Long) tuple.getKey();
            if (time > System.currentTimeMillis())
               break;
            times.add(time);
            Object key = tuple.getValue();
            if (key instanceof List)
               keys.addAll((List) key);
            else
               keys.add(key);
         }
         for (Long time : times) {
            expiryTree.remove(time);
         }
      }

      if (!keys.isEmpty())
         log.debug("purge (up to) " + keys.size() + " entries");
      int count = 0;
      for (Object key : keys) {
         InternalCacheEntry ice = (InternalCacheEntry) tree.find(key);
         if (ice == null)
            continue;
         if (ice.isExpired()) {
            // somewhat inefficient to FIND then REMOVE...
            tree.remove(key);
            count++;
         }
      }
      if (count != 0)
         log.debug("purged " + count + " entries");
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
      BTree bt = tree;
      BTree et = expiryTree;
      int size = (bt == null) ? -1 : bt.size();
      int expiry = (et == null) ? -1 : et.size();
      return "JdbmCacheLoader locationStr=" + config.getLocation() + " size=" + size + " expirySize=" + expiry;
   }

   private final class BTreeSet extends AbstractSet<InternalCacheEntry> {

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         final TupleBrowser browse;
         try {
            browse = tree.browse();
         } catch (IOException e) {
            throw new CacheException(e);
         }

         return new Iterator<InternalCacheEntry>() {

            final Tuple tuple = new Tuple();
            InternalCacheEntry current = null;
            boolean next = true;

            public boolean hasNext() {
               if (current == null) {
                  try {
                     next = browse.getNext(tuple);
                     current = (InternalCacheEntry) tuple.getValue();
                  } catch (IOException e) {
                     throw new CacheException(e);
                  }
               }
               return next;
            }

            public InternalCacheEntry next() {
               if (!hasNext())
                  throw new NoSuchElementException();
               try {
                  return current;
               } finally {
                  current = null;
               }
            }

            public void remove() {
               throw new UnsupportedOperationException();
            }

         };
      }

      @Override
      public int size() {
         return tree.size();
      }
   }
}