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

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.bridgemissing.NoFailureAsyncReplWarnFailurePolicyTest")
public class NoFailureAsyncReplWarnFailurePolicyTest extends BaseSiteUnreachableTest {

   public NoFailureAsyncReplWarnFailurePolicyTest() {
      lonBackupStrategy = BackupConfiguration.BackupStrategy.SYNC;
      lonBackupFailurePolicy = BackupFailurePolicy.WARN;
   }

   public void testNoFailures() {
      cache("LON", 0).put("k", "v");
      assertEquals(cache("LON", 0).get("k"), "v");
      assertEquals(cache("LON", 1).get("k"), "v");

      cache("LON", 1).remove("k");
      assertNull(cache("LON", 0).get("k"));
      assertNull(cache("LON", 1).get("k"));

      cache("LON", 0).putAll(Collections.singletonMap("k", "v"));
      assertEquals(cache("LON", 0).get("k"), "v");
      assertEquals(cache("LON", 1).get("k"), "v");
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }
}
