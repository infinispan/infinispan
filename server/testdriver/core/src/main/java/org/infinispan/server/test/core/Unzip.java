package org.infinispan.server.test.core;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Unzip {
   public static List<Path> unzip(Path zip, Path directory) throws IOException {
      List<Path> files = new ArrayList<>();
      Files.createDirectories(directory);
      try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(zip))) {
         ZipEntry entry = zipIn.getNextEntry();
         while (entry != null) {
            Path file = directory.resolve(entry.getName());
            if (entry.isDirectory()) {
               Files.createDirectories(file);
            } else {
               try (OutputStream fos = Files.newOutputStream(file)) {
                  byte[] bytesIn = new byte[4096];
                  int read;
                  while ((read = zipIn.read(bytesIn)) != -1) {
                     fos.write(bytesIn, 0, read);
                  }
               }
               files.add(file);
            }
            zipIn.closeEntry();
            entry = zipIn.getNextEntry();
         }
      }
      return files;
   }
}
