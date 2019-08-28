package org.infinispan.commons.jdkspecific;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * JDK 10+ implementation
 *
 * @author Tristan Tarrant
 */
public class ProcessorInfo {

   private ProcessorInfo() {
   }

   public static int availableProcessors() {
      int javaProcs = Runtime.getRuntime().availableProcessors();
      int userProcs = Integer.getInteger("infinispan.activeprocessorcount", javaProcs);
      return Math.min(userProcs, javaProcs);
   }

}
