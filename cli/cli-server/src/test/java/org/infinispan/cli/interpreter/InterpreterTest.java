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

import java.util.Map;

import static org.testng.AssertJUnit.*;

import org.infinispan.Cache;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.cli.interpreter.Interpreter;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.cli.interpreter.statement.CacheStatement;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName="cli-server.InterpreterTest")
public class InterpreterTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.jmxStatistics().enable().dataContainer().invocationBatching().enable();
      return TestCacheManagerFactory.createCacheManager(c);
   }

   private Interpreter getInterpreter() {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      return interpreter;
   }

   public void testSimple() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "put 'a' 'b'; get 'a';");
      interpreter
            .execute(sessionId, "put 'c' {\"org.infinispan.cli.interpreter.MyClass\":{\"i\":5,\"x\":null,\"b\":true}};");
      Object o = cache.get("c");
      assert o != null;
      assert o instanceof MyClass;
      assert ((MyClass) o).i == 5;
      assert ((MyClass) o).b;
      interpreter.execute(sessionId, "put 'f' 0.5;");
      Double f = (Double) cache.get("f");
      assert f == 0.5;
   }

   public void testPutIfAbsent() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "put 'a' 'a'; put --ifabsent 'a' 'b';");
      assertEquals("a", (String)cache.get("a"));
   }

   public void testCacheQualifier() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      Cache<Object, Object> otherCache = cacheManager.getCache("otherCache");
      interpreter.execute(sessionId, "put 'a' 'a'; put 'otherCache'.'b' 'b'; cache 'otherCache'; put 'c' 'c';");
      Object a = cache.get("a");
      assert a.equals("a");
      Object b = otherCache.get("b");
      assert b.equals("b");
      Object c = otherCache.get("c");
      assert c.equals("c");
   }

   public void testBatching() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "start; put 'a' 'a'; put 'b' 'b'; end;");
      Object a = cache.get("a");
      assert a.equals("a");
      Object b = cache.get("b");
      assert b.equals("b");
   }

   public void testTx() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "begin; put 'a' 'a'; commit;");
      Object a = cache.get("a");
      assert a.equals("a");
      interpreter.execute(sessionId, "begin; put 'b' 'b'; rollback;");
      assert !cache.containsKey("b");
   }

   public void testDangling() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "begin; put 'a' 'a';");
      assert cache.getAdvancedCache().getTransactionManager().getTransaction() == null;
      assert !cache.containsKey("a");
      interpreter.execute(sessionId, "start; put 'a' 'a';");
      assert cache.getAdvancedCache().getBatchContainer().getBatchTransaction() == null;
      assert !cache.containsKey("a");
   }

   public void testRemove() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "put 'a' 'a';");
      Object a = cache.get("a");
      assert a.equals("a");
      interpreter.execute(sessionId, "remove 'a';");
      assert !cache.containsKey("a");
      interpreter.execute(sessionId, "put 'b' 'b';");
      Object b = cache.get("b");
      assert b.equals("b");
      interpreter.execute(sessionId, "remove 'b' 'c';");
      assert cache.containsKey("b");
   }

   public void testReplace() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "put 'a' 'a';");
      Object a = cache.get("a");
      assert a.equals("a");
      interpreter.execute(sessionId, "replace 'a' 'b';");
      a = cache.get("a");
      assert a.equals("b");
      interpreter.execute(sessionId, "replace 'a' 'b' 'c';");
      a = cache.get("a");
      assert a.equals("c");
      interpreter.execute(sessionId, "replace 'a' 'b' 'd';");
      a = cache.get("a");
      assert a.equals("c");
   }

   public void testCreateLocal() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "create newcache;");
      assert cacheManager.cacheExists("newcache");
      interpreter.execute(sessionId, "create anothercache like newcache;");
      assert cacheManager.cacheExists("anothercache");
   }

   public void testUpgrade() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "upgrade --dumpkeys;");
   }

   public void testInvalidSession() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = "123";
      Map<String, String> response = interpreter.execute(sessionId, "put 'a' 'a';");
      assert response.containsKey(ResultKeys.ERROR.toString());
   }

   public void testCacheNotYetSelected() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(null);
      Map<String, String> response = interpreter.execute(sessionId, "cache;");
      assert response.containsKey(ResultKeys.ERROR.toString());
      String errorMsg = LogFactory.getLog(CacheStatement.class, Log.class).noCacheSelectedYet().getMessage();
      assert response.get(ResultKeys.ERROR.toString()).contains(errorMsg);
   }

   public void testStats() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      Map<String, String> response = interpreter.execute(sessionId, "stats;");
      assert !response.containsKey(ResultKeys.ERROR.toString());
      response = interpreter.execute(sessionId, "stats --container;");
      assert !response.containsKey(ResultKeys.ERROR.toString());
   }

   public void testParserErrors() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      Map<String, String> response = interpreter.execute(sessionId, "got a;");
      assertTrue(response.containsKey(ResultKeys.ERROR.toString()));
   }
}
