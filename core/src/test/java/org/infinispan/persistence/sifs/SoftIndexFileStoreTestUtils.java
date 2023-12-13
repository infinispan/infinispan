package org.infinispan.persistence.sifs;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class SoftIndexFileStoreTestUtils {

   public static long dataDirectorySize(String tmpDirectory, String cacheName) {
      Path dataPath = FileSystems.getDefault().getPath(tmpDirectory, "data", cacheName, "data");
      File[] dataFiles = dataPath.toFile().listFiles();

      long length = 0;
      for (File file : dataFiles) {
         length += file.length();
      }
      return length;
   }
}
