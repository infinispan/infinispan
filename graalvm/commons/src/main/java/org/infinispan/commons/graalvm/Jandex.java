package org.infinispan.commons.graalvm;

import java.io.File;
import java.io.IOException;
import java.security.CodeSource;
import java.util.Arrays;

import org.jboss.jandex.CompositeIndex;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.Indexer;
import org.jboss.jandex.JarIndexer;

public class Jandex {
   public static IndexView createIndex(File jar)  {
      Indexer indexer = new Indexer();
      try {
         return JarIndexer.createJarIndex(jar, indexer, false, false, false).getIndex();
      } catch (IOException e) {
         throw new IllegalStateException(String.format("Unable to create Jandex index for '%s'", jar), e);
      }
   }

   public static IndexView createIndex(CodeSource codeSource) {
      String jarPath = codeSource.getLocation().getFile();
      return createIndex(new File(jarPath));
   }

   public static IndexView createIndex(Class<?>... classes) {
      return CompositeIndex.create(
            Arrays.stream(classes)
                  .map(c -> c.getProtectionDomain().getCodeSource())
                  .map(Jandex::createIndex)
                  .toArray(IndexView[]::new)
      );
   }
}
