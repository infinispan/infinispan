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
      String lonCache = cache("LON", 0).getName();
      String lonSessionId = lonInterpreter.createSessionId(lonCache);
      Interpreter nycInterpreter = interpreter("NYC", 0);
      String nycCache = cache("NYC", 0).getName();
      String nycSessionId = nycInterpreter.createSessionId(nycCache);

      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --status NYC;", "online");

      assertInterpreterOutput(nycInterpreter, nycSessionId, String.format("site --status %s.LON;", lonCache), "online");

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
