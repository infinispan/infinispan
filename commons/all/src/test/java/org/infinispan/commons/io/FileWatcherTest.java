package org.infinispan.commons.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Eventually;
import org.junit.Test;

/**
 * @since 15.0
 **/
public class FileWatcherTest {

   @Test
   public void testFileWatcher() throws IOException, InterruptedException {
      try(FileWatcher watcher = new FileWatcher()) {
         Path tmpDir = Paths.get(CommonsTestingUtil.tmpDirectory(FileWatcherTest.class));
         Files.createDirectories(tmpDir);
         Path a = tmpDir.resolve("a");
         Files.deleteIfExists(a);
         Path b = tmpDir.resolve("b");
         Files.deleteIfExists(b);
         Files.createFile(a);
         ConcurrentMap<Path, AtomicInteger> counters = new ConcurrentHashMap<>();
         watcher.watch(a, p -> counters.computeIfAbsent(p, k -> new AtomicInteger()).incrementAndGet());
         Files.setLastModifiedTime(a, FileTime.fromMillis(System.currentTimeMillis()));
         Eventually.eventually(() -> counters.containsKey(a) && counters.get(a).get() == 1);
         Files.setLastModifiedTime(a, FileTime.fromMillis(System.currentTimeMillis()));
         Eventually.eventually(() -> counters.get(a).get() == 2);
         Files.createFile(b);
         Thread.sleep(FileWatcher.SLEEP + 500);
         assertFalse(counters.containsKey(b));
         assertEquals(2, counters.get(a).get());
         // watch "b"
         watcher.watch(b, p -> counters.computeIfAbsent(p, k -> new AtomicInteger()).incrementAndGet());
         // add a second watcher to "a"
         watcher.watch(a, p -> counters.computeIfAbsent(p, k -> new AtomicInteger()).incrementAndGet());
         Files.setLastModifiedTime(a, FileTime.fromMillis(System.currentTimeMillis()));
         Files.setLastModifiedTime(b, FileTime.fromMillis(System.currentTimeMillis()));
         Eventually.eventually(() -> counters.get(a).get() == 4);
         Eventually.eventually(() -> counters.containsKey(b) && counters.get(b).get() == 1);
      }
   }
}
