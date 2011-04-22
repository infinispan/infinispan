/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tester class for {@link JdbcMixedCacheStoreConfig}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "loaders.jdbc.mixed.JdbcMixedCacheStoreConfigTest")
public class JdbcMixedCacheStoreConfigTest {
   private JdbcMixedCacheStoreConfig config;

   @BeforeMethod
   public void createConfig() {
      config = new JdbcMixedCacheStoreConfig();
   }

   /**
    * Just take some random props and check their correctness.
    */
   public void simpleTest() {
      config = new JdbcMixedCacheStoreConfig();
      config.setConnectionUrl("url");
      config.setCreateTableOnStartForBinary(false);
      config.setCreateTableOnStartForStrings(true);
      config.setDataColumnNameForBinary("binary_dc");
      config.setDataColumnNameForStrings("strings_dc");
      config.setDataColumnTypeForBinary("binary_dct");
      config.setDataColumnTypeForStrings("strings_dct");
      config.setDriverClass("driver");

      //some checks
      assert !config.getBinaryCacheStoreConfig().getTableManipulation().isCreateTableOnStart();
      assert config.getStringCacheStoreConfig().getTableManipulation().isCreateTableOnStart();
      assert config.getConnectionFactoryConfig().getDriverClass().equals("driver");
      assert config.getBinaryCacheStoreConfig().getTableManipulation().getDataColumnName().equals("binary_dc");
      assert config.getBinaryCacheStoreConfig().getTableManipulation().getDataColumnType().equals("binary_dct");
      assert config.getStringCacheStoreConfig().getTableManipulation().getDataColumnName().equals("strings_dc");
      assert config.getStringCacheStoreConfig().getTableManipulation().getDataColumnType().equals("strings_dct");
   }

   public void testSameTableName() {
      config.setTableNamePrefixForBinary("table");
      try {
         config.setTableNamePrefixForStrings("table");
         assert false : "exception expected as same table name is not allowed for both cache stores";
      } catch (Exception e) {
         //expected
      }
      //and the other way around
      config.setTableNamePrefixForStrings("table2");
      try {
         config.setTableNamePrefixForBinary("table2");
         assert false : "exception expected as same table name is not allowed for both cache stores";
      } catch (Exception e) {
         //expected
      }
   }

   public void testKey2StringMapper() {
      config.setKey2StringMapperClass(DefaultTwoWayKey2StringMapper.class.getName());
      assert config.getStringCacheStoreConfig().getKey2StringMapper().getClass().equals(DefaultTwoWayKey2StringMapper.class);
   }

   public void testConcurrencyLevel() {
      assert config.getStringCacheStoreConfig().getLockConcurrencyLevel() == LockSupportCacheStoreConfig.DEFAULT_CONCURRENCY_LEVEL / 2;
      assert config.getBinaryCacheStoreConfig().getLockConcurrencyLevel() == LockSupportCacheStoreConfig.DEFAULT_CONCURRENCY_LEVEL / 2;
      config.setLockConcurrencyLevelForStrings(11);
      config.setLockConcurrencyLevelForBinary(12);
      assert config.getStringCacheStoreConfig().getLockConcurrencyLevel() == 11;
      assert config.getBinaryCacheStoreConfig().getLockConcurrencyLevel() == 12;
   }

   public void testEnforcedSyncPurging() {
      assert config.getBinaryCacheStoreConfig().isPurgeSynchronously();
      assert config.getStringCacheStoreConfig().isPurgeSynchronously();
   }

   public void voidTestLockAcquisitionTimeout() {
      assert config.getStringCacheStoreConfig().getLockAcquistionTimeout() == LockSupportCacheStoreConfig.DEFAULT_LOCK_ACQUISITION_TIMEOUT;
      assert config.getBinaryCacheStoreConfig().getLockAcquistionTimeout() == LockSupportCacheStoreConfig.DEFAULT_LOCK_ACQUISITION_TIMEOUT;
      config.setLockAcquistionTimeout(13);
      assert config.getStringCacheStoreConfig().getLockAcquistionTimeout() == 13;
      assert config.getBinaryCacheStoreConfig().getLockAcquistionTimeout() == 13;
   }
}
