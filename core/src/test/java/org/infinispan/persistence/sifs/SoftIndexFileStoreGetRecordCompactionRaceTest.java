package org.infinispan.persistence.sifs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Regression tests for the race between a reader obtaining a file position (from either the
 * index or the temporary table) and the compactor deleting that file before the reader opens it.
 *
 * <h2>Index path (issue #18036)</h2>
 * After the SoftBPlusTree refactor, {@code Index.getRecord} resolves the leaf via
 * {@code tree.get(indexKey)} (which has its own outdated-retry loop) and then calls
 * {@code entry.loadRecord(...)} <em>outside</em> that retry loop. If the compactor deletes the
 * backing data file between those two calls, {@code IndexEntry.ensureRecord} throws
 * {@code IndexNodeOutdatedException}, which is NOT retried and surfaces as a
 * {@code PersistenceException}.
 *
 * <h2>Temporary-table path</h2>
 * {@code NonBlockingSoftIndexFileStore.load()} and {@code containsKey()} both check the
 * temporary table first and, if they find a position there, open the file directly via
 * {@code fileProvider.getFile()}. If the file has been deleted between the table lookup and the
 * open, both paths must handle a {@code null} handle correctly:
 * <ul>
 *   <li>{@code load()} returns {@code null} from {@code readValueFromFileOffset}, which causes
 *       the outer {@code for(;;)} to retry — eventually reaching the index once the temp-table
 *       entry is removed. This is already graceful, but the test documents the behaviour.
 *   <li>{@code containsKey()} previously had an infinite busy-spin: a {@code null} handle
 *       caused it to loop back to the top without removing the stale temp-table entry or
 *       falling through to the index path.
 * </ul>
 *
 * <p>All races are made deterministic by {@link BlockingFileProvider}, which pauses any
 * {@code getFile()} call while armed, allowing the test to delete the file before releasing.
 */
@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreGetRecordCompactionRaceTest")
public class SoftIndexFileStoreGetRecordCompactionRaceTest extends SingleCacheManagerTest {
   private static final Log log = Log.getLog(SoftIndexFileStoreGetRecordCompactionRaceTest.class);

   private String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   @Override
   protected void createBeforeClass() throws Throwable {
      tmpDirectory = Testing.tmpDirectory(getClass());
      super.createBeforeClass();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().enable().persistentLocation(Testing.tmpDirectory(this.getClass()));

      // Single cache segment so all entries share one index-segment tree.
      // Small maxFileSize pushes entries across multiple complete data files quickly.
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().hash().numSegments(1);
      builder.persistence()
            .addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .maxFileSize(200);

      return TestCacheManagerFactory.newDefaultCacheManager(true, global, builder);
   }

   /**
    * Reproduces the {@code IndexNodeOutdatedException} regression from issue #18036.
    *
    * <p>Step-by-step:
    * <ol>
    *   <li>Write a {@code "target"} key plus enough filler to create multiple complete data files.
    *   <li>Delete filler so the compactor has entries to move and files to delete.
    *   <li>Look up the {@link IndexEntry} for {@code "target"} directly from the B+ tree and
    *       clear its {@code record} cache via reflection. This guarantees the next
    *       {@code store.load()} must call {@code dataFileProvider.getFile()} inside
    *       {@code IndexEntry.ensureRecord()} — the exact race window described in the issue.
    *   <li>Replace {@code Index.dataFileProvider} with a {@link BlockingFileProvider} that, when
    *       armed, blocks the first {@code getFile()} call it sees.
    *   <li>Arm the provider and fork {@code store.load("target")}. The fork reaches
    *       {@code ensureRecord()} and blocks at {@code getFile()}.
    *   <li>While the fork is suspended at the race window, run
    *       {@link Compactor#forceCompactionForAllNonLogFiles()} synchronously. The compactor
    *       moves {@code "target"} to a new data file and calls
    *       {@code realFileProvider.deleteFile(oldFile)}.
    *   <li>Release the fork. It delegates {@code getFile(oldFile)} to the real provider, which
    *       now returns {@code null} → {@code IndexNodeOutdatedException} → (without fix)
    *       {@code PersistenceException}.
    * </ol>
    */
   @Test
   public void testGetRecordThrowsWhenFileDeletedBetweenTreeGetAndLoadRecord() throws Exception {
      Cache<String, String> cache = cacheManager.getCache();

      // ── 1. Write data ────────────────────────────────────────────────────────────────────────
      final String targetKey = "target";
      cache.put(targetKey, "value-target");

      // Large-ish filler values so we exceed maxFileSize (200 bytes) quickly and create several
      // complete (non-log) files.
      String filler = "x".repeat(150);
      int fillerCount = 20;
      for (int i = 0; i < fillerCount; i++) {
         cache.put("filler-" + i, filler + i);
      }
      // Delete filler so files are mostly free → eligible for compaction.
      for (int i = 0; i < fillerCount; i++) {
         cache.remove("filler-" + i);
      }

      // ── 2. Extract store internals ────────────────────────────────────────────────────────────
      NonBlockingSoftIndexFileStore<String, String> store = TestingUtil.getFirstStore(cache);
      Compactor compactor = TestingUtil.extractField(store, "compactor");
      Index index = TestingUtil.extractField(store, "index");
      Marshaller marshaller = TestingUtil.extractField(store, "marshaller");

      // ── 3. Locate and evict the cached IndexEntry record ─────────────────────────────────────
      // Get the serialised key the same way NonBlockingSoftIndexFileStore does.
      byte[] serializedKey = Index.toIndexKey(marshaller.objectToBuffer(targetKey));

      // getInfo() returns the live IndexEntry object from the B+ tree (cast is safe: the
      // concrete type is always IndexEntry inside the Index).
      IndexEntry indexEntry = (IndexEntry) index.getInfo(targetKey, 0, serializedKey);
      assertNotNull(indexEntry, "IndexEntry for target key must be present");

      // Clear the cached EntryRecord so the next loadRecord() MUST call getFile()
      // inside ensureRecord() rather than returning the cached value immediately.
      // (record may already be null if the entry was never read; clearing is a no-op then.)
      TestingUtil.replaceField(null, "record", indexEntry, IndexEntry.class);

      // ── 4. Install intercepting FileProvider ─────────────────────────────────────────────────
      FileProvider realFileProvider = TestingUtil.extractField(index, "dataFileProvider");
      Path dataDir = ((File) TestingUtil.extractField(realFileProvider, "directoryFile")).toPath();

      BlockingFileProvider blockingFP = new BlockingFileProvider(realFileProvider, dataDir);
      // Index.dataFileProvider is final but setAccessible() still works for user classes.
      TestingUtil.replaceField(blockingFP, "dataFileProvider", index, Index.class);

      try {
         // ── 5. Arm and fork ──────────────────────────────────────────────────────────────────
         blockingFP.arm();

         // The fork calls: Index.getRecord() → tree.get() [returns IndexEntry with null record]
         //   → entry.loadRecord() → ensureRecord() → getFile() → BLOCKED HERE
         Future<MarshallableEntry<String, String>> readFuture =
               fork(() -> store.load(0, targetKey).toCompletableFuture().get(15, TimeUnit.SECONDS));

         // ── 6. Wait for reader to reach getFile() ────────────────────────────────────────────
         assertTrue(blockingFP.awaitBlocked(10, TimeUnit.SECONDS),
               "Reader thread did not reach getFile() within 10 seconds. "
                     + "Verify that IndexEntry.record was null before the fork.");

         // ── 7. Compact while reader is frozen ────────────────────────────────────────────────
         // The compactor moves "target" to a new data file. The compaction request completes
         // once index updates are done, but the actual file deletion is scheduled asynchronously
         // on the index processor AFTER the request completes. We therefore explicitly delete
         // the old file ourselves after compaction — the reader is still blocked, so it will
         // see the null handle when released.
         int oldFileId = indexEntry.file;
         compactor.forceCompactionForAllNonLogFiles()
               .toCompletableFuture().get(10, TimeUnit.SECONDS);

         // Explicitly delete the old data file — simulating what deleteFileAsync does.
         // The reader is still frozen at BlockingFileProvider.getFile(), so deleting now
         // means the real provider will return null when the latch is released.
         realFileProvider.deleteFile(oldFileId);

         // ── 8. Release and assert ─────────────────────────────────────────────────────────────
         // The reader now delegates getFile(oldFileId) to the real provider → null →
         // IndexNodeOutdatedException → (without fix) PersistenceException.
         blockingFP.release();

         try {
            MarshallableEntry<String, String> result = readFuture.get(10, TimeUnit.SECONDS);
            // Reaching here means the fix is applied: the retry loop re-resolved the entry.
            log.infof("Read succeeded with fix applied: key=%s", result != null ? result.getKey() : null);
         } catch (ExecutionException ex) {
            // Unwrap ExecutionException layers from CompletableFuture.get() inside the fork.
            Throwable cause = ex.getCause();
            while (cause instanceof ExecutionException) {
               cause = cause.getCause();
            }
            assertTrue(cause instanceof PersistenceException,
                  "Expected PersistenceException (bug is present) but got: " + cause);
            // Verify the root cause chain contains IndexNodeOutdatedException.
            Throwable root = cause;
            while (root != null && !(root instanceof org.infinispan.util.SoftBPlusTree.IndexNodeOutdatedException)) {
               root = root.getCause();
            }
            assertTrue(root instanceof org.infinispan.util.SoftBPlusTree.IndexNodeOutdatedException,
                  "Expected IndexNodeOutdatedException in cause chain but got: " + cause);
            throw new AssertionError(
                  "[BUG #18036] IndexNodeOutdatedException from Index.getRecord() was not retried "
                        + "and surfaced as PersistenceException. "
                        + "Fix: extend the outdated-retry loop to cover tree.get() + loadRecord() together.",
                  ex);
         }
      } finally {
         // Always release the reader (no-op if already released) and restore the real provider.
         blockingFP.release();
         TestingUtil.replaceField(realFileProvider, "dataFileProvider", index, Index.class);
      }
   }

   // =========================================================================
   // Temporary-table race tests
   // =========================================================================

   /**
    * Verifies that {@code load()} correctly retries when it catches a temp-table entry whose
    * backing file has just been deleted by the compactor.
    *
    * <p>The invariant maintained by {@link TemporaryTable#replaceOrLock}: the compactor updates
    * the temp-table entry to the new file location <em>before</em> deleting the old file. A
    * reader that observes a null handle for the old file is guaranteed to see the updated entry
    * on its next pass through the {@code for(;;)} loop. The fix ensures {@code load()} retries
    * in this case rather than falling through to the index with a stale picture.
    *
    * <p>The race is reproduced deterministically:
    * <ol>
    *   <li>Write {@code "target"} and enough filler to produce multiple complete data files.
    *   <li>Delete filler so the compactor has files to compact.
    *   <li>Plant a synthetic temp-table entry for {@code "target"} pointing to a deleted file,
    *       simulating the moment the reader observes the old location.
    *   <li>Fork {@code store.load()} — it will spin on the null handle.
    *   <li>From the test thread, update the entry (simulating {@code replaceOrLock}) to point
    *       to a valid file by removing the synthetic entry so the reader falls through to the
    *       index where the real entry still lives.
    *   <li>Assert the read succeeds and returns the correct value.
    * </ol>
    */
   @Test
   public void testLoadWithTemporaryTableEntryForDeletedFile() throws Exception {
      Cache<String, String> cache = cacheManager.getCache();
      final String targetKey = "tmp-target";
      cache.put(targetKey, "value-for-tmp-target");

      // Write filler to produce completed data files, then compact them away.
      String filler = "x".repeat(150);
      for (int i = 0; i < 20; i++) {
         cache.put("tmp-filler-" + i, filler + i);
      }
      for (int i = 0; i < 20; i++) {
         cache.remove("tmp-filler-" + i);
      }

      NonBlockingSoftIndexFileStore<String, String> store = TestingUtil.getFirstStore(cache);
      Compactor compactor = TestingUtil.extractField(store, "compactor");
      TemporaryTable temporaryTable = TestingUtil.extractField(store, "temporaryTable");
      FileProvider fileProvider = TestingUtil.extractField(store, "fileProvider");

      compactor.forceCompactionForAllNonLogFiles()
            .toCompletableFuture().get(10, TimeUnit.SECONDS);

      // File 0 was the first created and has been compacted away — use it as the "old" file.
      int deletedFile = 0;
      fileProvider.deleteFile(deletedFile); // no-op if already gone; ensures it is absent

      // Plant a stale temp-table entry for targetKey pointing at the deleted file.
      // This simulates the window where the reader observes the old location before
      // replaceOrLock has been seen.
      temporaryTable.set(0, targetKey, deletedFile, 0);

      // Fork the load — it will loop on the null handle until we fix up the entry.
      Future<MarshallableEntry<String, String>> future =
            fork(() -> store.load(0, targetKey).toCompletableFuture().get(15, TimeUnit.SECONDS));

      // Briefly let the reader spin, then simulate replaceOrLock completing: remove the
      // stale entry so the next retry sees no temp-table entry and falls through to the
      // index, which has the authoritative location.
      Thread.sleep(50);
      temporaryTable.removeConditionally(0, targetKey, deletedFile, 0);

      MarshallableEntry<String, String> result = future.get(10, TimeUnit.SECONDS);
      assertNotNull(result, "load() must find the entry after temp-table entry is updated");
   }

   /**
    * Verifies that {@code containsKey()} correctly retries when it catches a temp-table entry
    * whose backing file has just been deleted by the compactor.
    *
    * <p>Mirrors {@link #testLoadWithTemporaryTableEntryForDeletedFile}: the fix ensures the
    * {@code for(;;)} loop in {@code containsKey()} retries on a null handle so the reader
    * eventually sees the updated temp-table entry rather than spinning on stale data forever.
    */
   @Test
   public void testContainsKeyWithTemporaryTableEntryForDeletedFile() throws Exception {
      Cache<String, String> cache = cacheManager.getCache();
      final String targetKey = "ck-target";
      cache.put(targetKey, "value-for-ck-target");

      // Write filler to produce completed data files, then compact them away.
      String filler = "x".repeat(150);
      for (int i = 0; i < 20; i++) {
         cache.put("ck-filler-" + i, filler + i);
      }
      for (int i = 0; i < 20; i++) {
         cache.remove("ck-filler-" + i);
      }

      NonBlockingSoftIndexFileStore<String, String> store = TestingUtil.getFirstStore(cache);
      Compactor compactor = TestingUtil.extractField(store, "compactor");
      TemporaryTable temporaryTable = TestingUtil.extractField(store, "temporaryTable");
      FileProvider fileProvider = TestingUtil.extractField(store, "fileProvider");

      compactor.forceCompactionForAllNonLogFiles()
            .toCompletableFuture().get(10, TimeUnit.SECONDS);

      int deletedFile = 0;
      fileProvider.deleteFile(deletedFile);

      // Plant a stale temp-table entry for targetKey pointing at the deleted file.
      temporaryTable.set(0, targetKey, deletedFile, 0);

      // Fork containsKey — it will loop on the null handle until the entry is updated.
      Future<Boolean> future = fork(() ->
            store.containsKey(0, targetKey).toCompletableFuture().get(15, TimeUnit.SECONDS));

      // Simulate replaceOrLock completing: remove the stale entry so the reader falls through
      // to the index on its next retry.
      Thread.sleep(50);
      temporaryTable.removeConditionally(0, targetKey, deletedFile, 0);

      Boolean result = future.get(10, TimeUnit.SECONDS);
      assertTrue(result != null && result,
            "containsKey() must return true after temp-table entry is updated");
   }

   // =========================================================================
   // Test helper
   // =========================================================================

   /**
    * A {@link FileProvider} decorator that, while {@link #arm() armed}, intercepts the first
    * {@link #getFile(int)} call and blocks it until {@link #release()} is called. All calls
    * are then forwarded to the wrapped real provider.
    *
    * <p>One-shot: after the first armed call is taken (and blocked), subsequent calls pass
    * through immediately regardless of whether {@code release()} has been called.
    */
   static class BlockingFileProvider extends FileProvider {
      private final FileProvider delegate;
      /** Counts down when the reader enters {@code getFile()} and is waiting. */
      private final CountDownLatch blockedLatch = new CountDownLatch(1);
      /** Counting down this latch unblocks the waiting {@code getFile()} call. */
      private final CountDownLatch releaseLatch = new CountDownLatch(1);
      /**
       * One-shot arm flag. {@code true} → next {@code getFile()} call will block and flip it
       * to {@code false} (so only one call ever blocks).
       */
      private final AtomicBoolean armed = new AtomicBoolean(false);

      BlockingFileProvider(FileProvider delegate, Path directory) {
         // The super-constructor creates the directory (idempotent) and sets up internal state
         // that we never use — we override the only method this test cares about.
         super(directory, 1, "blocking.", Integer.MAX_VALUE, false);
         this.delegate = delegate;
      }

      /** Arms the one-shot block so the next {@link #getFile(int)} call will pause. */
      void arm() {
         armed.set(true);
      }

      /**
       * Blocks the calling thread until a reader thread has entered and is paused inside
       * {@link #getFile(int)}.
       *
       * @return {@code true} if the reader arrived within the given timeout
       */
      boolean awaitBlocked(long timeout, TimeUnit unit) throws InterruptedException {
         return blockedLatch.await(timeout, unit);
      }

      /** Releases any thread currently blocked inside {@link #getFile(int)}. */
      void release() {
         releaseLatch.countDown();
      }

      @Override
      public Handle getFile(int fileId) throws IOException {
         if (armed.compareAndSet(true, false)) {
            // Notify the test that we have entered the race window.
            blockedLatch.countDown();
            // Suspend until the test has completed compaction and calls release().
            try {
               releaseLatch.await();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new IOException("Interrupted while waiting for latch release", e);
            }
         }
         // Delegate to the real provider — may return null if the file was deleted.
         return delegate.getFile(fileId);
      }
   }
}
