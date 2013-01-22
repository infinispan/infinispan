/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.lucene.cachestore;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.FSDirectory;
import org.infinispan.Cache;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.lucene.IndexScopedKey;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.lucene.logging.Log;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.LogFactory;

/**
 * A CacheLoader meant to load Lucene index(es) from filesystem based Lucene index(es).
 * This is exclusively suitable for keys being used by the {@link InfinispanDirectory}, any other key
 * will be ignored.
 * 
 * The InfinispanDirectory requires indexes to be named; this CacheLoader needs to be configured
 * with the path of the root directory containing the indexes, and expects index names to match directory
 * names under this common root path.
 * 
 * @author Sanne Grinovero
 * @since 5.2
 */
@CacheLoaderMetadata(configurationClass = LuceneCacheLoaderConfig.class)
public class LuceneCacheLoader implements CacheLoader {

   private static final Log log = LogFactory.getLog(LuceneCacheLoader.class, Log.class);

   private final ConcurrentHashMap<String,DirectoryLoaderAdaptor> openDirectories = new ConcurrentHashMap<String, DirectoryLoaderAdaptor>();
   private String fileRoot;
   private File rootDirectory;
   private int autoChunkSize;

   @Override
   public void init(final CacheLoaderConfig config, final Cache<?, ?> cache, final StreamingMarshaller m) throws CacheLoaderException {
      LuceneCacheLoaderConfig cfg = (LuceneCacheLoaderConfig) config;
      this.fileRoot = cfg.location;
      this.autoChunkSize = cfg.autoChunkSize;
   }

   @Override
   public InternalCacheEntry load(final Object key) throws CacheLoaderException {
      if (key instanceof IndexScopedKey) {
         final IndexScopedKey indexKey = (IndexScopedKey)key;
         DirectoryLoaderAdaptor directoryAdaptor = getDirectory(indexKey);
         Object value = directoryAdaptor.load(indexKey);
         if (value != null) {
            return new ImmortalCacheEntry(key, value);
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
   public boolean containsKey(final Object key) throws CacheLoaderException {
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
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return load(Integer.MAX_VALUE);
   }

   /**
    * Loads up to a specific number of entries.  There is no guarantee about the order of loaded entries.
    */
   @Override
   public Set<InternalCacheEntry> load(int maxEntries) throws CacheLoaderException {
      scanForUnknownDirectories();
      final HashSet<InternalCacheEntry> allInternalEntries = new HashSet<InternalCacheEntry>();
      for (DirectoryLoaderAdaptor dir : openDirectories.values()) {
         dir.loadAllEntries(allInternalEntries, maxEntries);
      }
      return allInternalEntries;
   }

   @Override
   public Set<Object> loadAllKeys(final Set keysToExclude) throws CacheLoaderException {
      scanForUnknownDirectories();
      final HashSet allKeys = new HashSet(); //filthy generic covariants!
      for (DirectoryLoaderAdaptor dir : openDirectories.values()) {
         dir.loadAllKeys(allKeys, keysToExclude);
      }
      return allKeys;
   }

   /**
    * There might be Directories we didn't store yet in the openDirectories Map.
    * Make sure they are all initialized before serving methods such as {@link #loadAll()}
    * or {@link #loadAllKeys(Set)}.
    */
   private void scanForUnknownDirectories() {
      File[] filesInRoot = rootDirectory.listFiles();
      for (File maybeDirectory : filesInRoot) {
         if (maybeDirectory.isDirectory()) {
            String name = maybeDirectory.getName();
            try {
               getDirectory(name);
            } catch (CacheLoaderException e) {
               log.couldNotWalkDirectory(name, e);
            }
         }
      }
   }

   @Override
   public void start() throws CacheLoaderException {
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
   public void stop() throws CacheLoaderException {
      for (Entry<String, DirectoryLoaderAdaptor> entry : openDirectories.entrySet()) {
         DirectoryLoaderAdaptor directory = entry.getValue();
         directory.close();
      }
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return LuceneCacheLoaderConfig.class;
   }

   private DirectoryLoaderAdaptor getDirectory(final IndexScopedKey indexKey) throws CacheLoaderException {
      final String indexName = indexKey.getIndexName();
      return getDirectory(indexName);
   }

   /**
    * Looks up the Directory adapter if it's already known, or attempts to initialize indexes.
    */
   private DirectoryLoaderAdaptor getDirectory(final String indexName) throws CacheLoaderException {
      DirectoryLoaderAdaptor adapter = openDirectories.get(indexName);
      if (adapter == null) {
         synchronized (openDirectories) {
            adapter = openDirectories.get(indexName);
            if (adapter == null) {
               final File path = new File(this.rootDirectory, indexName);
               final FSDirectory directory = openLuceneDirectory(path);
               adapter = new DirectoryLoaderAdaptor(directory, indexName, autoChunkSize);
               openDirectories.put(indexName, adapter);
            }
         }
      }
      return adapter;
   }

   /**
    * Attempts to open a Lucene FSDirectory on the specified path
    */
   private FSDirectory openLuceneDirectory(final File path) throws CacheLoaderException {
      try {
         return FSDirectory.open(path);
      } catch (IOException e) {
         throw log.exceptionInCacheLoader(e);
      }
   }

}
