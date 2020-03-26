package org.infinispan.commons.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.CopyOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.Consumer;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class CommonsTestingUtil {
   public static final String TEST_PATH = "infinispanTempFiles";

   /**
    * Creates a path to a unique (per test) temporary directory. By default, the directory is created in the platform's
    * temp directory, but the location can be overridden with the {@code infinispan.test.tmpdir} system property.
    *
    * @param test test that requires this directory.
    * @return an absolute path
    */
   public static String tmpDirectory(Class<?> test) {
      return Paths.get(tmpDirectory(), TEST_PATH, test.getSimpleName()).toString();
   }

   /**
    * See {@link CommonsTestingUtil#tmpDirectory(Class)}
    *
    * @return an absolute path
    */
   public static String tmpDirectory(String... folders) {
      String[] tFolders = new String[folders.length + 1];
      tFolders[0] = TEST_PATH;
      System.arraycopy(folders, 0, tFolders, 1, folders.length);
      return Paths.get(tmpDirectory(), tFolders).toString();
   }

   public static String tmpDirectory() {
      return System.getProperty("infinispan.test.tmpdir", System.getProperty("java.io.tmpdir"));
   }

   /**
    * Creates a path to a unique (per test) temporary directory. The directory is created in the platform's temp
    * directory (set by {@code java.io.tmpdir}).
    *
    * @param test test that requires this directory.
    * @return an absolute path
    */
   public static String javaTmpDirectory(Class<?> test) {
      return System.getProperty("java.io.tmpdir") + separator + TEST_PATH + separator + test.getSimpleName();
   }

   public static String loadFileAsString(InputStream is) throws IOException {
      StringBuilder sb = new StringBuilder();
      BufferedReader r = new BufferedReader(new InputStreamReader(is));
      for (String line = r.readLine(); line != null; line = r.readLine()) {
         sb.append(line);
         sb.append("\n");
      }
      return sb.toString();
   }

   public static class CopyFileVisitor extends SimpleFileVisitor<Path> {
      private final Path targetPath;
      private Path sourcePath = null;
      private final CopyOption[] copyOptions;
      private final Consumer<File> manipulator;

      public CopyFileVisitor(Path targetPath, boolean overwrite) {
         this(targetPath, overwrite, null);
      }

      public CopyFileVisitor(Path targetPath, boolean overwrite, Consumer<File> manipulator) {
         this.targetPath = targetPath;
         this.manipulator = manipulator;
         this.copyOptions = overwrite ? new CopyOption[]{StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES} : new CopyOption[0];
      }

      @Override
      public FileVisitResult preVisitDirectory(final Path dir,
                                               final BasicFileAttributes attrs) throws IOException {
         if (sourcePath == null) {
            sourcePath = dir;
         } else {
            Files.createDirectories(targetPath.resolve(sourcePath.relativize(dir).toString()));
         }
         return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(final Path file,
                                       final BasicFileAttributes attrs) throws IOException {
         Path target = targetPath.resolve(sourcePath.relativize(file).toString());
         Files.copy(file, target, copyOptions);
         if (manipulator != null) {
            manipulator.accept(target.toFile());
         }
         return FileVisitResult.CONTINUE;
      }
   }
}
