package org.infinispan.commons.test.junit;

import java.io.File;

import org.junit.rules.TemporaryFolder;

/**
 * Extend JUnit's TemporaryFolder to use system property {@code infinispan.test.tmpdir}.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class TmpDir extends TemporaryFolder {
   public static final String BASE_DIR =
      System.getProperty("infinispan.test.tmpdir", System.getProperty("java.io.tmpdir"));

   public TmpDir() {
      super(new File(BASE_DIR));
   }
}
