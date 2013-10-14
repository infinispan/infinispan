package org.infinispan.lucene.cachestore;

import org.apache.lucene.store.FSDirectory;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.lucene.IndexScopedKey;
import org.infinispan.lucene.cachestore.configuration.LuceneStoreConfiguration;
import org.infinispan.lucene.logging.Log;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

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
public class LuceneCacheLoader implements AdvancedCacheLoader {

   private static final Log log = LogFactory.getLog(LuceneCacheLoader.class, Log.class);

   private final ConcurrentHashMap<String,DirectoryLoaderAdaptor> openDirectories = new ConcurrentHashMap<String, DirectoryLoaderAdaptor>();
   private String fileRoot;
   private File rootDirectory;
   private int autoChunkSize;

   private LuceneStoreConfiguration configuration;
   private InitializationContext ctx;


   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.fileRoot = this.configuration.location();
      this.autoChunkSize = this.configuration.autoChunkSize();
   }

   @Override
   public MarshalledEntry load(final Object key) throws CacheLoaderException {
      if (key instanceof IndexScopedKey) {
         final IndexScopedKey indexKey = (IndexScopedKey)key;
         DirectoryLoaderAdaptor directoryAdaptor = getDirectory(indexKey);
         Object value = directoryAdaptor.load(indexKey);
         if (value != null) {
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, value, null);
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
   public boolean contains(final Object key) throws CacheLoaderException {
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
   public void process(final KeyFilter filter, final CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      scanForUnknownDirectories();
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);

      final TaskContextImpl taskContext = new TaskContextImpl();
      for (final DirectoryLoaderAdaptor dir : openDirectories.values()) {
         eacs.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               try {
                  final HashSet<MarshalledEntry> allInternalEntries = new HashSet<MarshalledEntry>();
                  dir.loadAllEntries(allInternalEntries, Integer.MAX_VALUE, ctx.getMarshaller());
                  for (MarshalledEntry me : allInternalEntries) {
                     if (taskContext.isStopped())
                        break;
                     if (filter == null || filter.shouldLoadKey(me.getKey())) {
                        task.processEntry(me, taskContext);
                     }
                  }
                  return null;
               } catch (Exception e) {
                  log.errorExecutingParallelStoreTask(e);
                  throw e;
               }
            }
         });
      }
      eacs.waitUntilAllCompleted();
      if (eacs.isExceptionThrown()) {
         throw new CacheLoaderException("Execution exception!", eacs.getFirstException());
      }
   }

   @Override
   public int size() {
      return PersistenceUtil.count(this, null);
   }

   /**
    * There might be Directories we didn't store yet in the openDirectories Map.
    * Make sure they are all initialized before serving methods such as {@link #process(org.infinispan.persistence.spi.AdvancedCacheLoader.KeyFilter, org.infinispan.persistence.spi.AdvancedCacheLoader.CacheLoaderTask, java.util.concurrent.Executor, boolean, boolean)}
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
               final InternalDirectoryContract wrapped = ContractAdaptorFactory.wrapNativeDirectory(directory);
               adapter = new DirectoryLoaderAdaptor(wrapped, indexName, autoChunkSize);
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
