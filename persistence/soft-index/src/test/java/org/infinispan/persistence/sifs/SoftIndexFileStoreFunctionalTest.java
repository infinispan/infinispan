package org.infinispan.persistence.sifs;

import java.io.File;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "unit", testName = "persistence.SoftIndexFileStoreFunctionalTest")
public class SoftIndexFileStoreFunctionalTest extends BaseStoreFunctionalTest {
   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }


   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      persistence.addStore(SoftIndexFileStoreConfigurationBuilder.class).preload(preload).dataLocation(tmpDirectory + "/data").indexLocation(tmpDirectory);
      return persistence;
   }
}
