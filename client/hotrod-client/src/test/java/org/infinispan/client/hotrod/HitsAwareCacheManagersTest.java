package org.infinispan.client.hotrod;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.test.TestingUtil.*;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class HitsAwareCacheManagersTest extends MultipleCacheManagersTest {

   protected Map<SocketAddress, HotRodServer> addr2hrServer = new LinkedHashMap<SocketAddress, HotRodServer>();
   protected List<RemoteCacheManager> clients = new ArrayList<RemoteCacheManager>();

   protected void createHotRodServers(int num, ConfigurationBuilder defaultBuilder) {
      // Start Hot Rod servers
      for (int i = 0; i < num; i++) addHotRodServer(defaultBuilder);
      // Verify that default caches should be started
      for (int i = 0; i < num; i++) assert manager(i).getCache() != null;
      // Block until views have been received
      blockUntilViewReceived(manager(0).getCache(), num);
      // Verify that caches running
      for (int i = 0; i < num; i++) {
         blockUntilCacheStatusAchieved(
            manager(i).getCache(), ComponentStatus.RUNNING, 10000);
      }

      // Add hits tracking interceptors
      addInterceptors();

      for (int i = 0; i < num; i++) {
         clients.add(createClient());
      }
   }

   protected RemoteCacheManager client(int i) {
      return clients.get(i);
   }

   protected RemoteCacheManager createClient() {
      return new RemoteCacheManager(createHotRodClientConfigurationBuilder(
         addr2hrServer.values().iterator().next().getPort()).build());
   }

   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer()
         .host("localhost")
         .port(serverPort)
         .pingOnStartup(false);
      return clientBuilder;
   }

   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(builder);
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm);
      InetSocketAddress addr = new InetSocketAddress(server.getHost(), server.getPort());
      addr2hrServer.put(addr, server);
      return server;
   }

   protected HotRodServer addHotRodServer(ConfigurationBuilder builder, int port) {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(builder);
      HotRodServer server = HotRodTestingUtil.startHotRodServer(
         cm, port, new HotRodServerConfigurationBuilder());
      InetSocketAddress addr = new InetSocketAddress(server.getHost(), server.getPort());
      addr2hrServer.put(addr, server);
      return server;
   }

   protected void killServer() {
      Iterator<HotRodServer> it = addr2hrServer.values().iterator();
      HotRodServer server = it.next();
      EmbeddedCacheManager cm = server.getCacheManager();
      it.remove();
      killServers(server);
      killCacheManagers(cm);
      cacheManagers.remove(cm);
   }

   @Override
   @BeforeMethod(alwaysRun=true)
   public void createBeforeMethod() throws Throwable {
      if (cleanupAfterMethod()) {
         addr2hrServer.clear();
      }
      super.createBeforeMethod();
   }

   protected HitCountInterceptor getHitCountInterceptor(Cache<?, ?> cache) {
      HitCountInterceptor hitCountInterceptor = null;
      List<CommandInterceptor> interceptorChain = cache.getAdvancedCache().getInterceptorChain();
      for (CommandInterceptor interceptor : interceptorChain) {
         boolean isHitCountInterceptor = interceptor instanceof HitCountInterceptor;
         if (hitCountInterceptor != null && isHitCountInterceptor) {
            throw new IllegalStateException("Two HitCountInterceptors! " + interceptorChain);
         }
         if (isHitCountInterceptor) {
            hitCountInterceptor = (HitCountInterceptor) interceptor;
         }
      }
      return hitCountInterceptor;
   }

   protected void assertOnlyServerHit(SocketAddress serverAddress) {
      assertServerHit(serverAddress, null, 1);
   }

   protected void assertServerHit(SocketAddress serverAddress, String cacheName, int expectedHits) {
      CacheContainer cacheContainer = addr2hrServer.get(serverAddress).getCacheManager();
      HitCountInterceptor interceptor = getHitCountInterceptor(namedCache(cacheName, cacheContainer));
      assert interceptor.getHits() == expectedHits :
            "Expected " + expectedHits + " hit(s) for " + serverAddress + " but received " + interceptor.getHits();
      for (HotRodServer server : addr2hrServer.values()) {
         if (server.getCacheManager() != cacheContainer) {
            interceptor = getHitCountInterceptor(namedCache(cacheName, server.getCacheManager()));
            assert interceptor.getHits() == 0 :
                  "Expected 0 hits in " + serverAddress + " but got " + interceptor.getHits();
         }
      }
   }

   private Cache<?, ?> namedCache(String cacheName, CacheContainer cacheContainer) {
      return cacheName == null ? cacheContainer.getCache() : cacheContainer.getCache(cacheName);
   }

   protected void assertNoHits() {
      for (HotRodServer server : addr2hrServer.values()) {
         HitCountInterceptor interceptor = getHitCountInterceptor(server.getCacheManager().getCache());
         assert interceptor.getHits() == 0 : "Expected 0 hits but got " + interceptor.getHits();
      }
   }

   protected InetSocketAddress getAddress(HotRodServer hotRodServer) {
      InetSocketAddress socketAddress = new InetSocketAddress(hotRodServer.getHost(), hotRodServer.getPort());
      addr2hrServer.put(socketAddress, hotRodServer);
      return socketAddress;
   }

   protected void resetStats() {
      for (EmbeddedCacheManager manager : cacheManagers) {
         HitCountInterceptor cmi = getHitCountInterceptor(manager.getCache());
         cmi.reset();
      }
   }

   protected void addInterceptors() {
      addInterceptors(null);
   }

   protected void addInterceptors(String cacheName) {
      for (EmbeddedCacheManager manager : cacheManagers) {
         Cache<?, ?> cache = namedCache(cacheName, manager);
         addHitCountInterceptor(cache);
      }
   }

   private void addHitCountInterceptor(Cache<?, ?> cache) {
      HitCountInterceptor interceptor = new HitCountInterceptor();
      cache.getAdvancedCache().addInterceptor(interceptor, 1);
   }

   /**
    * @author Mircea.Markus@jboss.com
    * @since 4.1
    */
   public static class HitCountInterceptor extends CommandInterceptor{
      private static final Log log = LogFactory.getLog(HitCountInterceptor.class);

      private volatile int invocationCount;

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         if (ctx.isOriginLocal()) {
            log.infof("Increment hit count after being visited by %s", command);
            invocationCount ++;
         }
         return super.handleDefault(ctx, command);
      }

      public int getHits() {
         return invocationCount;
      }

      public void reset() {
         invocationCount = 0;
      }
   }
}
