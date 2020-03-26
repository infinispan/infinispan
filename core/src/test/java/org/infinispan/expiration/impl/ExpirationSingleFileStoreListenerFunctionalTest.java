package org.infinispan.expiration.impl;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationSingleFileStoreListenerFunctionalTest")
public class ExpirationSingleFileStoreListenerFunctionalTest extends ExpirationStoreListenerFunctionalTest {

   private final String location = CommonsTestingUtil.tmpDirectory(this.getClass());

   @Factory
   @Override
   public Object[] factory() {
      return new Object[]{
            // Test is for single file store with a listener in a local cache and we don't care about memory storage types
            new ExpirationSingleFileStoreListenerFunctionalTest().cacheMode(CacheMode.LOCAL),
      };
   }

   @Override
   protected String parameters() {
      return null;
   }

   @Override
   protected void configure(GlobalConfigurationBuilder globalBuilder) {
      globalBuilder.globalState().persistentLocation(location);
   }

   @Override
   protected void configure(ConfigurationBuilder config) {
      config
         // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
         .expiration().wakeUpInterval(Long.MAX_VALUE)
         .persistence().addSingleFileStore().location(location);
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(CommonsTestingUtil.tmpDirectory(this.getClass()));
   }
}
