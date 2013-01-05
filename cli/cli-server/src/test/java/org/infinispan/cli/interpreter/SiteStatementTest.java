/*
 * JBoss, Home of Professional Open Source
 *  Copyright 2012 Red Hat Inc. and/or its affiliates and other
 *  contributors as indicated by the @author tags. All rights reserved
 *  See the copyright.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  This is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as
 *  published by the Free Software Foundation; either version 2.1 of
 *  the License, or (at your option) any later version.
 *
 *  This software is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this software; if not, write to the Free
 *  Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 *  02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.cli.interpreter;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;

import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups = "xsite", testName = "cli-server.SiteStatementTest")
public class SiteStatementTest extends AbstractTwoSitesTest {

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   public void testSiteStatus() throws Exception {
      Interpreter lonInterpreter = interpreter("LON", 0);
      String lonSessionId = lonInterpreter.createSessionId(cache("LON", 0).getName());
      Interpreter nycInterpreter = interpreter("NYC", 0);
      String nycSessionId = nycInterpreter.createSessionId(cache("NYC", 0).getName());

      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --status NYC;", "online");

      assertInterpreterOutput(nycInterpreter, nycSessionId, "site --status LON;", "online");

      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --offline NYC;", "ok");

      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --online NYC;", "ok");

   }

   private void assertInterpreterOutput(Interpreter interpreter, String sessionId, String command, String output) throws Exception {
      Map<String, String> result = interpreter.execute(sessionId, command);
      assertEquals(output, result.get(ResultKeys.OUTPUT.toString()));
   }

   private Interpreter interpreter(String site, int cache) {
      return cache(site, cache).getAdvancedCache().getComponentRegistry().getComponent(Interpreter.class);
   }
}
