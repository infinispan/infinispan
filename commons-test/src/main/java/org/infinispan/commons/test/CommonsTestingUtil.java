package org.infinispan.commons.test;

import static java.io.File.separator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

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
      return tmpDirectory() + separator + TEST_PATH + separator + test.getSimpleName();
   }

   /**
    * See {@link CommonsTestingUtil#tmpDirectory(Class)}
    *
    * @return an absolute path
    */
   public static String tmpDirectory(String folder) {
      return tmpDirectory() + separator + TEST_PATH + separator + folder;
   }

   public static String tmpDirectory() {
      return System.getProperty("infinispan.test.tmpdir", System.getProperty("java.io.tmpdir"));
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
}
