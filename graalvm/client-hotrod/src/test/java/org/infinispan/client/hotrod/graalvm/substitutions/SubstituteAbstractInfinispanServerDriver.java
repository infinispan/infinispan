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
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

public class SubstituteAbstractInfinispanServerDriver {

   public static void copyFromJar(String source, final Path target) throws URISyntaxException, IOException {
      URI resource = SubstituteAbstractInfinispanServerDriver.class.getResource("").toURI();
      try (FileSystem fileSystem = FileSystems.newFileSystem(resource, Collections.emptyMap())) {
         final Path jarPath = fileSystem.getPath(source);
         Files.walkFileTree(jarPath, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
               Path currentTarget = target.resolve(jarPath.relativize(dir).toString());
               Files.createDirectories(currentTarget);
               return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
               Files.copy(file, target.resolve(jarPath.relativize(file).toString()), StandardCopyOption.REPLACE_EXISTING);
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
   protected void createKeyStores(String extension, String type, String providerName) {
      // no-op
   }
}
