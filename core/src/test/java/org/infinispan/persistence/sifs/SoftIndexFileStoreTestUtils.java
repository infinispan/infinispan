package org.infinispan.persistence.sifs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import org.infinispan.util.logging.Log;

public class SoftIndexFileStoreTestUtils {

   public static class StatsValue {
      private final long freeSize;
      private final long statsSize;

      public long getFreeSize() {
         return freeSize;
      }

      public long getStatsSize() {
         return statsSize;
      }

      private StatsValue(long freeSize, long statsSize) {
         this.freeSize = freeSize;
         this.statsSize = statsSize;
      }
   }

   public static StatsValue readStatsFile(String tmpDirectory, String cacheName, Log log) throws IOException {
      long statsSize = 0;
      long freeSize = 0;
      try (FileChannel statsChannel = new RandomAccessFile(
            Path.of(tmpDirectory, "index", cacheName, "index", "index.stats").toFile(), "r").getChannel()) {
         ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + 8);
         while (Index.read(statsChannel, buffer)) {
            buffer.flip();
            // Ignore id
            int file = buffer.getInt();
            int length = buffer.getInt();
            int free = buffer.getInt();
            // Ignore expiration
            buffer.getLong();
            buffer.flip();

            statsSize += length;
            freeSize += free;
            log.debugf("File: %s Length: %s free: %s", file, length, free);
         }
      }

      return new StatsValue(freeSize, statsSize);
   }

   public static long dataDirectorySize(String tmpDirectory, String cacheName) {
      Path dataPath = Path.of(tmpDirectory, "data", cacheName, "data");
      File[] dataFiles = dataPath.toFile().listFiles();

      long length = 0;
      for (File file : dataFiles) {
         length += file.length();
      }
      return length;
   }
}
