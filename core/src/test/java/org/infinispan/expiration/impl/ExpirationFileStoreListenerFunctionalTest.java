package org.infinispan.expiration.impl;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationFileStoreListenerFunctionalTest")
public class ExpirationFileStoreListenerFunctionalTest extends ExpirationStoreListenerFunctionalTest {

   private final String location = CommonsTestingUtil.tmpDirectory(this.getClass());

   private FileStoreToUse fileStoreToUse;

   ExpirationStoreFunctionalTest fileStoreToUse(FileStoreToUse fileStoreToUse) {
      this.fileStoreToUse = fileStoreToUse;
      return this;
   }

   enum FileStoreToUse {
      SINGLE,
      SOFT_INDEX
   }

   @Factory
   @Override
   public Object[] factory() {
      return new Object[]{
            new ExpirationFileStoreListenerFunctionalTest().fileStoreToUse(FileStoreToUse.SOFT_INDEX).cacheMode(CacheMode.LOCAL),
      };
   }

   @Override
   protected String parameters() {
      return "[ " + fileStoreToUse + "]";
   }

   @Override
   protected void configure(GlobalConfigurationBuilder globalBuilder) {
      super.configure(globalBuilder);
      globalBuilder.globalState().enable().persistentLocation(location);
   }

   @Override
   protected void configure(ConfigurationBuilder config) {
      config
            // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
            .expiration().wakeUpInterval(Long.MAX_VALUE);
      switch (fileStoreToUse) {
         case SOFT_INDEX:
            config.persistence().addSoftIndexFileStore().dataLocation(location);
            break;
      }
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(CommonsTestingUtil.tmpDirectory(this.getClass()));
   }
}
