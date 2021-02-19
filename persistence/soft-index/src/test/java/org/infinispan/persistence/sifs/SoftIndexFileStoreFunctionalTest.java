package org.infinispan.persistence.sifs;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "unit", testName = "persistence.SoftIndexFileStoreFunctionalTest")
public class SoftIndexFileStoreFunctionalTest extends BaseStoreFunctionalTest {
   private String tmpDirectory;

   @BeforeMethod
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }


   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      persistence.addStore(SoftIndexFileStoreConfigurationBuilder.class).preload(preload).dataLocation(tmpDirectory + "/data").indexLocation(tmpDirectory).segmented(false);
      return persistence;
   }
}
