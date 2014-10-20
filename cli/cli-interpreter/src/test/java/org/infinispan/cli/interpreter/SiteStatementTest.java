package org.infinispan.cli.interpreter;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
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

   public void testSiteStateTransfer() throws Exception {
      Interpreter lonInterpreter = interpreter("LON", 0);
      String lonCache = cache("LON", 0).getName();
      String lonSessionId = lonInterpreter.createSessionId(lonCache);
      Interpreter nycInterpreter = interpreter("NYC", 0);
      String nycCache = cache("NYC", 0).getName();
      String nycSessionId = nycInterpreter.createSessionId(nycCache);

      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --offline NYC;", "ok");
      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --status NYC;", "offline");
      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --push NYC;", "ok");

      assertEventuallyNoStateTransferInReceivingSite("NYC", nycCache, 10, TimeUnit.SECONDS);
      assertEventuallyNoStateTransferInSendingSite("LON", lonCache, 10, TimeUnit.SECONDS);

      assertInterpreterOutput(nycInterpreter, nycSessionId, "site --sendingsite;", "null");
      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --pushstatus;", "NYC=OK");
      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --clearpushstatus;", "ok");
      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --pushstatus;", null);
      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --cancelpush NYC;", "ok");
      assertInterpreterOutput(nycInterpreter, nycSessionId, "site --cancelreceive LON;", "ok");
   }

   private void assertInterpreterOutput(Interpreter interpreter, String sessionId, String command, String output) throws Exception {
      Map<String, String> result = interpreter.execute(sessionId, command);
      assertEquals(output, result.get(ResultKeys.OUTPUT.toString()));
   }

   private Interpreter interpreter(String site, int cache) {
      return cache(site, cache).getAdvancedCache().getComponentRegistry().getComponent(Interpreter.class);
   }

   private void assertEventuallyNoStateTransferInReceivingSite(String siteName, String cacheName, long timeout, TimeUnit unit) {
      assertEventuallyInSite(siteName, cacheName, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            CommitManager commitManager = extractComponent(cache, CommitManager.class);
            return !commitManager.isTracking(Flag.PUT_FOR_STATE_TRANSFER) &&
                  !commitManager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER) &&
                  commitManager.isEmpty();
         }
      }, timeout, unit);
   }

   private void assertEventuallyNoStateTransferInSendingSite(String siteName, String cacheName, long timeout, TimeUnit unit) {
      assertEventuallyInSite(siteName, cacheName, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            return extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty() &&
                  extractComponent(cache, XSiteStateTransferManager.class).getRunningStateTransfers().isEmpty();
         }
      }, timeout, unit);
   }

}
