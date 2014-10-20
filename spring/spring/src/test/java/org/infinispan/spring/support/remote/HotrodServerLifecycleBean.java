package org.infinispan.spring.support.remote;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * HotrodServerLifecycleBean.
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @since 5.1
 */
public class HotrodServerLifecycleBean implements InitializingBean, DisposableBean {

   private EmbeddedCacheManager cacheManager;

   private RemoteCacheManager remoteCacheManager;

   private HotRodServer hotrodServer;

   private String remoteCacheName;

   public void setRemoteCacheName(String remoteCacheName) {
      this.remoteCacheName = remoteCacheName;
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(false);
      cacheManager.getCache(remoteCacheName);

      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(builder.build());
   }

   @Override
   public void destroy() throws Exception {
      remoteCacheManager.stop();
      hotrodServer.stop();
      cacheManager.stop();
   }
}
