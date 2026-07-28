package org.infinispan.client.hotrod.graalvm.substitutions;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;

import org.infinispan.server.test.core.AbstractInfinispanServerDriver;
import org.infinispan.server.test.core.CertificateAuthority;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

public class SubstituteAbstractInfinispanServerDriver {

   public static void copyFromJar(String source, final Path target) throws URISyntaxException, IOException {
      URI resource = SubstituteAbstractInfinispanServerDriver.class.getResource("/").toURI();
      try (FileSystem fileSystem = FileSystems.newFileSystem(resource, Collections.emptyMap())) {
         final Path root = fileSystem.getRootDirectories().iterator().next();
         final String sourcePrefix = "/" + source;
         // Walk from root to work around GraalVM 25.2 bug where readAttributes()
         // fails on directory entries but cached attributes from directory listing work.
         // Use string-based relativization to avoid NativeImageResourcePath.relativize()
         // failing on paths with different resource indexes.
         Files.walkFileTree(root, new SimpleFileVisitor<>() {

            private String relativize(Path path) {
               String s = path.toString();
               if (s.equals(sourcePrefix)) {
                  return "";
               }
               return s.substring(sourcePrefix.length() + 1);
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
               String dirStr = dir.toString();
               if ("/".equals(dirStr)) {
                  return FileVisitResult.CONTINUE;
               }
               if (!dirStr.startsWith(sourcePrefix)) {
                  return FileVisitResult.SKIP_SUBTREE;
               }
               Path currentTarget = target.resolve(relativize(dir));
               Files.createDirectories(currentTarget);
               return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
               if (file.toString().startsWith(sourcePrefix)) {
                  Files.copy(file, target.resolve(relativize(file)), StandardCopyOption.REPLACE_EXISTING);
               }
               return FileVisitResult.CONTINUE;
            }
         });
      }
   }
}

@TargetClass(AbstractInfinispanServerDriver.class)
final class Target_AbstractInfinispanServerDriver {

   @Alias
   protected InfinispanServerTestConfiguration configuration;

   @Alias
   private File confDir;

   @Substitute
   private void copyProvidedServerConfigurationFile() {
      try {
         SubstituteAbstractInfinispanServerDriver.copyFromJar("configuration", confDir.toPath());
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   @Substitute
   protected void createKeyStores(CertificateAuthority.ExportType type) {
      // no-op
   }
}
