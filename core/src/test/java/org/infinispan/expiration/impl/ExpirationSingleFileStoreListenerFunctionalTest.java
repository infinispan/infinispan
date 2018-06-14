package org.infinispan.expiration.impl;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationSingleFileStoreListenerFunctionalTest")
public class ExpirationSingleFileStoreListenerFunctionalTest extends ExpirationStoreListenerFunctionalTest {
   @Override
   protected void configure(ConfigurationBuilder config) {
      config
         // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
         .expiration().wakeUpInterval(Long.MAX_VALUE)
         .persistence().addSingleFileStore().location(TestingUtil.tmpDirectory(this.getClass()));
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(TestingUtil.tmpDirectory(this.getClass()));
   }
}
