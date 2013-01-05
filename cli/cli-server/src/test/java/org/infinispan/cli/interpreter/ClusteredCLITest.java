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
package org.infinispan.cli.interpreter;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups="functional", testName = "cli-server.ClusteredCLITest")
public class ClusteredCLITest extends MultipleCacheManagersTest {

   public static String CACHE_NAME = "distCache";

   public ClusteredCLITest() {
      cleanup = CleanupPhase.AFTER_TEST;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(2, cacheName(), builder);
   }

   protected String cacheName() {
      return CACHE_NAME;
   }

   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

   public void testCreateCluster() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(manager(0));
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = interpreter.createSessionId(null);
      interpreter.execute(sessionId, String.format("create anothercache like %s;", cacheName()));
      assert manager(0).cacheExists("anothercache");
      assert manager(1).cacheExists("anothercache");
   }
}
