/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.xsite;

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
@Test(testName = "xsite.XSiteCacheConfigurationTest", groups = "functional, xsite")
public class XSiteCacheConfigurationTest {

   public void testApi() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.
         sites().addBackup()
            .site("LON")
            .strategy(BackupConfiguration.BackupStrategy.SYNC)
         .sites().addBackup()
            .site("SFO")
         .sites().addBackup()
            .site("NYC");
      assertEquals(cb.sites().backups().size(), 3);
      BackupConfigurationBuilder backup0 = cb.sites().backups().get(0);
      assertEquals(backup0.site(), "LON");
      assertEquals(backup0.strategy(), BackupConfiguration.BackupStrategy.SYNC);

      BackupConfigurationBuilder backup1 = cb.sites().backups().get(1);
      assertEquals(backup1.site(), "SFO");
      assertEquals(backup1.strategy(), BackupConfiguration.BackupStrategy.ASYNC);

      BackupConfigurationBuilder backup2 = cb.sites().backups().get(2);
      assertEquals(backup2.site(), "NYC");
      assertEquals(backup2.strategy(), BackupConfiguration.BackupStrategy.ASYNC);

      Configuration b = cb.build();
      assertEquals(b.sites().backups().size(), 3);
      BackupConfiguration b0 = b.sites().backups().get(0);
      assertEquals(b0.site(), "LON");
      assertEquals(b0.strategy(), BackupConfiguration.BackupStrategy.SYNC);

      BackupConfiguration b1 = b.sites().backups().get(1);
      assertEquals(b1.site(), "SFO");
      assertEquals(b1.strategy(), BackupConfiguration.BackupStrategy.ASYNC);

      BackupConfigurationBuilder b2 = cb.sites().backups().get(2);
      assertEquals(b2.site(), "NYC");
      assertEquals(b2.strategy(), BackupConfiguration.BackupStrategy.ASYNC);
   }

   @Test (expectedExceptions = ConfigurationException.class)
   public void testSameBackupDefinedMultipleTimes() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.
            sites().addBackup()
               .site("LON")
               .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addBackup()
               .site("LON")
            .sites().addBackup()
               .site("NYC");
      cb.build();
   }

   public void testMultipleCachesWithNoCacheName() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.
            sites().addBackup()
               .site("LON")
               .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addBackup()
               .site("SFO")
               .sites().addBackup()
            .site("NYC");
      cb.build();
   }
}
