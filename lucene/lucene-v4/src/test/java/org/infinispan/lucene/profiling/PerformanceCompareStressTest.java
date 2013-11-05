package org.infinispan.lucene.profiling;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.infinispan.Cache;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.DirectoryIntegrityCheck;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
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
@SuppressWarnings("unchecked")
@Test(groups = "profiling", testName = "lucene.profiling.PerformanceCompareStressTest", sequential = true)
public class PerformanceCompareStressTest {

   /**
    * The number of terms in the dictionary used as source of terms by the IndexWriter to produce
    * new documents
    */
   private static final int DICTIONARY_SIZE = 800 * 1000;

   /** Concurrent Threads in tests */
   private static final int READER_THREADS = 5;
   private static final int WRITER_THREADS = 1;

   private static final int CHUNK_SIZE = 512 * 1024;

   private static final String indexName = "tempIndexName";

   private long durationMs = 2 * 60 * 1000;

   private Cache cache;

   private EmbeddedCacheManager cacheFactory;
   private Properties results = null;
   private String currentMethod = null;

   @Test
   public void profileTestRAMDirectory() throws InterruptedException, IOException {
      RAMDirectory dir = new RAMDirectory();
      stressTestDirectoryInternal(dir, "RAMDirectory");
   }

   @Test
   public void profileTestFSDirectory() throws InterruptedException, IOException {
      File indexDir = new File(TestingUtil.tmpDirectory(this.getClass()), indexName);
      boolean directoriesCreated = indexDir.mkdirs();
      assert directoriesCreated : "couldn't create directory for FSDirectory test";
      FSDirectory dir = FSDirectory.open(indexDir);
      stressTestDirectoryInternal(dir, "FSDirectory");
   }

   @Test
   public void profileTestInfinispanDirectoryWithNetworkDelayZero() throws InterruptedException, IOException {
      // TestingUtil.setDelayForCache(cache, 0, 0);
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).chunkSize(CHUNK_SIZE).create();
      stressTestDirectoryInternal(dir, "InfinispanClustered-delayedIO:0");
      verifyDirectoryState();
   }

   @Test
   public void profileTestInfinispanDirectoryWithNetworkDelay4() throws Exception {
      TestingUtil.setDelayForCache(cache, 4, 4);
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).chunkSize(CHUNK_SIZE).create();
      stressTestDirectoryInternal(dir, "InfinispanClustered-delayedIO:4");
      verifyDirectoryState();
   }

   @Test
   public void profileTestInfinispanDirectoryWithHighNetworkDelay40() throws Exception {
      TestingUtil.setDelayForCache(cache, 40, 40);
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).chunkSize(CHUNK_SIZE).create();
      stressTestDirectoryInternal(dir, "InfinispanClustered-delayedIO:40");
      verifyDirectoryState();
   }

   @Test
   public void profileInfinispanLocalDirectory() throws InterruptedException, IOException {
      CacheContainer cacheContainer = CacheTestSupport.createLocalCacheManager();
      try {
         cache = cacheContainer.getCache();
         Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).chunkSize(CHUNK_SIZE).create();
         stressTestDirectoryInternal(dir, "InfinispanLocal");
         verifyDirectoryState();
      } finally {
         cacheContainer.stop();
      }
   }

   @Test(enabled=false)//to prevent invocations from some versions of TestNG
   public static void stressTestDirectory(Directory dir, String testLabel) throws InterruptedException, IOException {
      stressTestDirectory(dir, testLabel, 120000l, null, null);
   }

   private void stressTestDirectoryInternal(Directory dir, String testLabel) throws InterruptedException, IOException {
      stressTestDirectory(dir, testLabel, durationMs, results, currentMethod);
   }

   @Test(enabled=false)//to prevent invocations from some versions of TestNG
   private static void stressTestDirectory(Directory dir, String testLabel, long durationMs, Properties results, String currentMethod) throws InterruptedException, IOException {
      SharedState state = new SharedState(DICTIONARY_SIZE);
      CacheTestSupport.initializeDirectory(dir);
      ExecutorService e = Executors.newFixedThreadPool(READER_THREADS + WRITER_THREADS);
      for (int i = 0; i < READER_THREADS; i++) {
         e.execute(new LuceneReaderThread(dir, state));
      }
      for (int i = 0; i < WRITER_THREADS; i++) {
         e.execute(new LuceneWriterThread(dir, state));
      }
      e.shutdown();
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
   public void beforeTest() {
      cacheFactory = TestCacheManagerFactory.createClusteredCacheManager(
            CacheTestSupport.createTestConfiguration(TransactionMode.NON_TRANSACTIONAL));
      cacheFactory.start();
      cache = cacheFactory.getCache();
      cache.clear();
   }

   @AfterMethod
   public void afterTest() {
      TestingUtil.killCaches(cache);
      TestingUtil.killCacheManagers(cacheFactory);
      TestingUtil.recursiveFileRemove(indexName);
   }

   private void verifyDirectoryState() {
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, indexName, true);
   }

   /**
    * It's much better to compare performance out of the scope of TestNG by
    * running this directly as TestNG enables assertions.
    *
    * Suggested test switches:
    * -Xmx2G -Xms2G -XX:MaxPermSize=128M -XX:+HeapDumpOnOutOfMemoryError -Xss512k -XX:HeapDumpPath=/tmp/java_heap -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=127.0.0.1 -Xbatch -server -XX:+UseCompressedOops -XX:+UseLargePages -XX:LargePageSizeInBytes=2m -XX:+AlwaysPreTouch
    */
   public static void main(String[] args) throws Exception {
      String[] testMethods = System.getProperty("lucene.profiling.tests",
            "profileTestRAMDirectory,profileTestFSDirectory,profileInfinispanLocalDirectory,profileTestInfinispanDirectoryWithNetworkDelayZero").split(",");
      PerformanceCompareStressTest test = new PerformanceCompareStressTest();
      test.durationMs = new Long(System.getProperty("lucene.profiling.duration", "120000"));
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
