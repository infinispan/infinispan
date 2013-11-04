package org.infinispan.compatibility.adaptor52x;

import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.persistence.file.SingleFileStore;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "compatibility.adaptor52x.FileStoreTest")
public class FileStoreTest extends Adaptor52xCustomLoaderTest {

   public FileStoreTest() {
      configurationFile = "52x-file-store.xml";
   }

   public void testLocationIsCorrect() {
      SingleFileStoreConfiguration c = ((SingleFileStore) cl).getConfiguration();
      assertEquals(c.location(), DIR);
   }
}
