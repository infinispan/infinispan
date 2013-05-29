/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.xsite;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.BackupCacheStoppedTest")
public class BackupCacheStoppedTest extends AbstractTwoSitesTest {

   public void testCacheStopped() {
      final String site = "LON";
      String key = key(site);
      String val = val(site);

      cache(site, 0).put(key, val);
      Cache<Object,Object> backup = backup(site);
      final GlobalComponentRegistry gcr = backup.getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry();

      assertEquals(backup.get(key), val);
      assertTrue(backup.getStatus().allowInvocations());

      backup.stop();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            BackupReceiverRepositoryImpl component = (BackupReceiverRepositoryImpl) gcr.getComponent(BackupReceiverRepository.class);
            return component.getBackupReceiver(site, EmbeddedCacheManager.DEFAULT_CACHE_NAME) == null;
         }
      });

      assertFalse(backup.getStatus().allowInvocations());

      backup.start();

      log.trace("About to put the 2nd value");
      cache(site, 0).put(key, "v2");
      assertEquals(backup(site).get(key), "v2");
      assertTrue(backup.getStatus().allowInvocations());
   }

   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }
}

