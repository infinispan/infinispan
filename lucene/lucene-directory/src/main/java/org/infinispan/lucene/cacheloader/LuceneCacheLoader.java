package org.infinispan.lucene.cacheloader;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import org.apache.lucene.store.FSDirectory;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.lucene.IndexScopedKey;
import org.infinispan.lucene.cacheloader.configuration.LuceneLoaderConfiguration;
import org.infinispan.lucene.logging.Log;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

/**
 * A CacheLoader meant to load Lucene index(es) from filesystem based Lucene index(es).
 * This is exclusively suitable for keys being used by the Directory, any other key
 * will be ignored.
 *
 * The InfinispanDirectory requires indexes to be named; this CacheLoader needs to be configured
 * with the path of the root directory containing the indexes, and expects index names to match directory
 * names under this common root path.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
@ConfiguredBy(LuceneLoaderConfiguration.class)
public class LuceneCacheLoader<K, V> implements AdvancedCacheLoader<K, V> {

   private static final Log log = LogFactory.getLog(LuceneCacheLoader.class, Log.class);

   private final ConcurrentHashMap<String,DirectoryLoaderAdaptor> openDirectories = new ConcurrentHashMap<>();
   private String fileRoot;
   private File rootDirectory;
   private int autoChunkSize;
   private int affinitySegmentId;

   private InitializationContext ctx;


   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      LuceneLoaderConfiguration configuration = ctx.getConfiguration();
      this.fileRoot = configuration.location();
      this.autoChunkSize = configuration.autoChunkSize();
      this.affinitySegmentId = configuration.affinitySegmentId();
   }

   @Override
   public MarshallableEntry loadEntry(final Object key) {
      if (key instanceof IndexScopedKey) {
         final IndexScopedKey indexKey = (IndexScopedKey)key;
         DirectoryLoaderAdaptor directoryAdaptor = getDirectory(indexKey);
         Object value = directoryAdaptor.load(indexKey);
         if (value != null) {
            return ctx.getMarshallableEntryFactory().create(key, value, null);
         }
         else {
            return null;
         }
      }
      else {
         log.cacheLoaderIgnoringKey(key);
         return null;
      }
   }

   @Override
   public boolean contains(final Object key) {
      if (key instanceof IndexScopedKey) {
         final IndexScopedKey indexKey = (IndexScopedKey)key;
         final DirectoryLoaderAdaptor directoryAdaptor = getDirectory(indexKey);
         return directoryAdaptor.containsKey(indexKey);
      }
      else {
         log.cacheLoaderIgnoringKey(key);
         return false;
      }
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> entryPublisher(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      return Flowable.defer(() -> {
         // Make sure that we update directories before we start iterating upon directories
         scanForUnknownDirectories();
         return Flowable.fromIterable(openDirectories.values());
      })
            // We parallelize this since the loading below is blocking
            .parallel()
            .runOn(Schedulers.from(ctx.getExecutor()))
            .flatMap(dir -> {
               final Set<MarshallableEntry<K, V>> allInternalEntries = new HashSet<>();
               dir.loadAllEntries(allInternalEntries, Integer.MAX_VALUE, ctx.getMarshallableEntryFactory());
               return Flowable.fromIterable(allInternalEntries);
            })
            .filter(me -> filter == null || filter.test(me.getKey()))
            .sequential();
   }

   @Override
   public int size() {
      return PersistenceUtil.count(this, null);
   }

   /**
    * There might be Directories we didn't store yet in the openDirectories Map.
    * Make sure they are all initialized before serving methods such as {@link #publishEntries(Predicate, boolean, boolean)}
    */
   private void scanForUnknownDirectories() {
      File[] filesInRoot = rootDirectory.listFiles();
      if (filesInRoot != null) {
         for (File maybeDirectory : filesInRoot) {
            if (maybeDirectory.isDirectory()) {
               String name = maybeDirectory.getName();
               try {
                  getDirectory(name);
               }
               catch (PersistenceException e) {
                  log.couldNotWalkDirectory(name, e);
               }
            }
         }
      }
   }

   @Override
   public void start() {
      rootDirectory = new File(fileRoot);
      if (rootDirectory.exists()) {
         if (!rootDirectory.isDirectory() || ! rootDirectory.canRead()) {
            // we won't verify write capability to support read-only - should we have an explicit option for it?
            throw log.rootDirectoryIsNotADirectory(fileRoot);
         }
      }
      else {
         boolean mkdirsSuccess = rootDirectory.mkdirs();
         if (!mkdirsSuccess) {
            throw log.unableToCreateDirectory(fileRoot);
         }
      }
   }

   @Override
   public void stop() {
      for (Entry<String, DirectoryLoaderAdaptor> entry : openDirectories.entrySet()) {
         DirectoryLoaderAdaptor directory = entry.getValue();
         directory.close();
      }
   }

   private DirectoryLoaderAdaptor getDirectory(final IndexScopedKey indexKey) {
      final String indexName = indexKey.getIndexName();
      return getDirectory(indexName);
   }

   /**
    * Looks up the Directory adapter if it's already known, or attempts to initialize indexes.
    */
   private DirectoryLoaderAdaptor getDirectory(final String indexName) {
      DirectoryLoaderAdaptor adapter = openDirectories.get(indexName);
      if (adapter == null) {
         synchronized (openDirectories) {
            adapter = openDirectories.get(indexName);
            if (adapter == null) {
               final File path = new File(this.rootDirectory, indexName);
               final FSDirectory directory = openLuceneDirectory(path);
               adapter = new DirectoryLoaderAdaptor(directory, indexName, autoChunkSize, affinitySegmentId);
               openDirectories.put(indexName, adapter);
            }
         }
      }
      return adapter;
   }

   /**
    * Attempts to open a Lucene FSDirectory on the specified path
    */
   private FSDirectory openLuceneDirectory(final File path) {
      try {
         return FSDirectory.open(path.toPath());
      }
      catch (IOException e) {
         throw log.exceptionInCacheLoader(e);
      }
   }

}
