package org.infinispan.lucene.profiling;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.infinispan.Cache;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.directory.BuildContext;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PerformanceCompareStressTest is useful to get an idea on relative performance between Infinispan
 * in local or clustered mode against a RAMDirectory or FSDirectory. To be reliable set a long
 * DURATION_MS and a number of threads similar to the use case you're interested in: results might
 * vary on the number of threads because of the lock differences. This is not meant as a benchmark
 * but used to detect regressions.
 *
 * This requires Lucene > 2.9.1 or Lucene > 3.0.0 because of
 * https://issues.apache.org/jira/browse/LUCENE-2095
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "profiling", testName = "lucene.profiling.PerformanceCompareStressTest", sequential = true)
public class PerformanceCompareStressTest {

   private static final int NUM_NODES = 4;
   private static final String CONFIGURATION = "perf-udp.xml";

   /**
    * The number of terms in the dictionary used as source of terms by the IndexWriter to produce
    * new documents
    */
   private static final int DICTIONARY_SIZE = 800 * 1000;

   /** Concurrent Threads in tests */
   private static final int READER_THREADS = 5;
   private static final int WRITER_THREADS = 1;

   private static final boolean INDEX_EXCLUSIVE = true;
   private static final int CHUNK_SIZE = 1024 * 1024;

   private static final String indexName = "tempIndexName";

   private static final long DEFAULT_DURATION_MS = 30 * 60 * 1000;
   private static final boolean ASYNC_METADATA_WRITES = true;
   private static final boolean ASYNC_DELETES = false;
   private static final int ASYNC_DELETES_POOL_SIZE = 10;
   private long durationMs = DEFAULT_DURATION_MS;

   private final Map<Integer,EmbeddedCacheManager> cacheManagers = new HashMap<>();

   private Properties results = null;
   private String currentMethod = null;

   @Test
   public void profileTestRAMDirectory() throws InterruptedException, IOException {
      RAMDirectory dir = new RAMDirectory();
      stressTestDirectoryInternal(dir, dir, "RAMDirectory");
   }

   @Test
   public void profileTestFSDirectory() throws InterruptedException, IOException {
      File indexDir = new File(TestingUtil.tmpDirectory(this.getClass()), indexName);
      boolean directoriesCreated = indexDir.mkdirs();
      assert directoriesCreated : "couldn't create directory for FSDirectory test";
      FSDirectory dir = FSDirectory.open(indexDir);
      stressTestDirectoryInternal(dir, dir, "FSDirectory");
   }

   @Test
   public void profileTestInfinispanDirectoryWithNetworkDelayZero() throws Exception {
      setNetworkDelay(0);
      Directory dir1 = buildDirectoryFromNode(1);
      Directory dir2 = buildDirectoryFromNode(3);
      stressTestDirectoryInternal(dir1, dir2, "InfinispanClustered-delayedIO:0");
      verifyDirectoryState();
   }

   @Test
   public void profileTestInfinispanDirectoryWithNetworkDelay1() throws Exception {
      setNetworkDelay(1);
      Directory dir1 = buildDirectoryFromNode(1);
      Directory dir2 = buildDirectoryFromNode(3);
      stressTestDirectoryInternal(dir1, dir2, "InfinispanClustered-delayedIO:1");
      verifyDirectoryState();
      setNetworkDelay(0);
   }

   @Test
   public void profileTestInfinispanDirectoryWithHighNetworkDelay4() throws Exception {
      setNetworkDelay(4);
      Directory dir1 = buildDirectoryFromNode(1);
      Directory dir2 = buildDirectoryFromNode(3);
      stressTestDirectoryInternal(dir1, dir2, "InfinispanClustered-delayedIO:4");
      verifyDirectoryState();
      setNetworkDelay(0);
   }

   @Test
   public void profileTestInfinispanDirectoryWithHighNetworkDelay20() throws Exception {
      setNetworkDelay(20);
      Directory dir1 = buildDirectoryFromNode(1);
      Directory dir2 = buildDirectoryFromNode(3);
      stressTestDirectoryInternal(dir1, dir2, "InfinispanClustered-delayedIO:20");
      verifyDirectoryState();
      setNetworkDelay(0);
   }

   @Test
   public void profileInfinispanLocalDirectory() throws InterruptedException, IOException {
      CacheContainer cacheContainer = CacheTestSupport.createLocalCacheManager();
      try {
         Cache cache = cacheContainer.getCache();
         Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).chunkSize(CHUNK_SIZE).create();
         stressTestDirectoryInternal(dir, dir, "InfinispanLocal");
         verifyDirectoryState();
      } finally {
         cacheContainer.stop();
      }
   }

   @Test(enabled=false)//to prevent invocations from some versions of TestNG
   public static void stressTestDirectory(Directory dir, String testLabel) throws InterruptedException, IOException {
      stressTestDirectory(dir, dir, testLabel, 120000l, null, null);
   }

   private void stressTestDirectoryInternal(Directory dirWriter, Directory dirReaders, String testLabel) throws InterruptedException, IOException {
      stressTestDirectory(dirWriter, dirReaders, testLabel, durationMs, results, currentMethod);
   }

   private void setNetworkDelay(int delay) throws Exception {
      for (int i=0; i<NUM_NODES; i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         //Any cache will do:
         TestingUtil.setDelayForCache(cm.getCache(), delay, delay);
      }
      System.out.println("Simulating network packet delay of: " + delay);
   }

   private ThreadFactory createThreadFactory() {
      return new ThreadFactory() {
         private AtomicInteger atomicInteger = new AtomicInteger(0);

         @Override
         public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setName("File-deleter-" + atomicInteger.incrementAndGet());
            return thread;
         }
      };
   }

   private Executor createDeleteExecutor() {
      return new ThreadPoolExecutor(
            0,
            ASYNC_DELETES_POOL_SIZE,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(),
            createThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy());
   }

   private Directory buildDirectoryFromNode(int node) {
      EmbeddedCacheManager cm = cacheManagers.get(node);
      BuildContext context = DirectoryBuilder
            .newDirectoryInstance(cm.getCache("index_metadata"), cm.getCache("index_data"), cm.getCache("index_locks"), indexName)
            .writeFileListAsynchronously(ASYNC_METADATA_WRITES)
            .chunkSize(CHUNK_SIZE);
      if (ASYNC_DELETES) {
         context.deleteOperationsExecutor(createDeleteExecutor());
      }
      return context.create();
   }

   @Test(enabled=false)//to prevent invocations from some versions of TestNG
   private static void stressTestDirectory(Directory dirWriter, Directory dirReaders, String testLabel, long durationMs, Properties results, String currentMethod) throws InterruptedException, IOException {
      SharedState state = new SharedState(DICTIONARY_SIZE);
      CacheTestSupport.initializeDirectory(dirWriter);
      ExecutorService e = Executors.newFixedThreadPool(READER_THREADS + WRITER_THREADS);
      for (int i = 0; i < READER_THREADS; i++) {
         e.execute(new LuceneReaderThread(dirReaders, state));
      }
      for (int i = 0; i < WRITER_THREADS; i++) {
         e.execute(INDEX_EXCLUSIVE ? new LuceneWriterExclusiveThread(dirWriter, state) : new LuceneWriterThread(dirWriter, state));
      }
      e.shutdown();
      System.out.println("Started test: " + testLabel);
      state.startWaitingThreads();
      Thread.sleep(durationMs);
      long searchesCount = state.incrementIndexSearchesCount(0);
      long writerTaskCount = state.incrementIndexWriterTaskCount(0);
      state.quit();
      boolean terminatedCorrectly = e.awaitTermination(20, TimeUnit.SECONDS);
      AssertJUnit.assertTrue(terminatedCorrectly);
      System.out.println("Test " + testLabel + " run in " + durationMs + "ms:\n\tSearches: " + searchesCount + "\n\t" + "Writes: "
               + writerTaskCount);
      if (results != null) {
         results.setProperty(currentMethod + ".label", testLabel);
         results.setProperty(currentMethod + ".searches", Long.toString(searchesCount));
         results.setProperty(currentMethod + ".writes", Long.toString(writerTaskCount));
      }
   }

   @BeforeMethod
   public void beforeTest() throws IOException {
      for (int i = 0; i < NUM_NODES; i++) {
         DefaultCacheManager node = new DefaultCacheManager(CONFIGURATION);
         node.start();
         //Start all its caches:
         node.getCache("index_metadata").start();
         node.getCache("index_data").start();
         node.getCache("index_locks").start();
         cacheManagers.put(i, node);
      }
   }

   @AfterMethod
   public void afterTest() {
      for (EmbeddedCacheManager node : cacheManagers.values()) {
         TestingUtil.killCacheManagers(node);
      }
      TestingUtil.recursiveFileRemove(indexName);
   }

   private void verifyDirectoryState() {
      for (EmbeddedCacheManager node : cacheManagers.values()) {
//         DirectoryIntegrityCheck.verifyDirectoryStructure(cache, indexName, true);
      }
   }

   /**
    * It's much better to compare performance out of the scope of TestNG by
    * running this directly as TestNG enables assertions.
    *
    * Select which tests to run:
    * -Dlucene.profiling.tests=profileInfinispanLocalDirectory,profileTestInfinispanDirectoryWithNetworkDelayZero
    *
    * Suggested test switches:
    * -Xmx4G -Xms4G -XX:MaxPermSize=128M -XX:+HeapDumpOnOutOfMemoryError -Xss512k -XX:HeapDumpPath=/tmp/java_heap -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=127.0.0.1 -XX:+UseLargePages -XX:LargePageSizeInBytes=2m
    *
    * With detailed GC logging:
    * -Xmx4G -Xms4G -XX:MaxPermSize=32M -XX:+HeapDumpOnOutOfMemoryError -Xss256k -XX:HeapDumpPath=/tmp/java_heap -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=127.0.0.1 -XX:+UseLargePages -XX:LargePageSizeInBytes=2m -XX:+UseLargePages -XX:LargePageSizeInBytes=2m -Xloggc:gc-full.log -XX:+PrintGCDetails -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime
    *
    * To enable flight recorder:
    * -XX:+UnlockCommercialFeatures -XX:+FlightRecorder
    */
   public static void main(String[] args) throws Exception {
      String[] testMethods = System.getProperty("lucene.profiling.tests",
            "profileTestRAMDirectory,profileTestFSDirectory,profileInfinispanLocalDirectory,profileTestInfinispanDirectoryWithNetworkDelayZero").split(",");
      PerformanceCompareStressTest test = new PerformanceCompareStressTest();
      test.durationMs = new Long(System.getProperty("lucene.profiling.duration", String.valueOf(DEFAULT_DURATION_MS)));
      String outputFile = System.getProperty("lucene.profiling.output");
      test.results = outputFile == null ? null : new Properties();
      for (String testMethod : testMethods) {
         try {
            test.currentMethod = testMethod;
            Method m = PerformanceCompareStressTest.class.getMethod(testMethod);
            test.beforeTest();
            try {
               m.invoke(test);
            } finally {
               test.afterTest();
            }
         } catch (NoSuchMethodException e) {
            System.out.println("Couldn't find method " + testMethod);
            System.exit(1);
         }
      }
      if (test.results != null && !test.results.isEmpty()) {
         System.out.println("Writing results to " + outputFile + " ...");
         TestingUtil.outputPropertiesToXML(outputFile, test.results);
      }
   }

}
