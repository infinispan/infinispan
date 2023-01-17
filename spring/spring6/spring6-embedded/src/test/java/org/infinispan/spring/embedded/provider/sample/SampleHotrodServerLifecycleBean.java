package org.infinispan.spring.embedded.provider.sample;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Starts test HotRod server instance with pre-defined set of caches.
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
public class SampleHotrodServerLifecycleBean implements InitializingBean, DisposableBean {

   private EmbeddedCacheManager cacheManager;

   private HotRodServer hotrodServer;

   private String remoteCacheName;

   private String remoteBackupCacheName;

   private String customCacheName;

   public void setRemoteCacheName(String remoteCacheName) {
      this.remoteCacheName = remoteCacheName;
   }

   public void setRemoteBackupCacheName(String remoteBackupCacheName) {
      this.remoteBackupCacheName = remoteBackupCacheName;
   }

   public void setCustomCacheName(String customCacheName) {
      this.customCacheName = customCacheName;
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(HotRodTestingUtil.hotRodCacheConfiguration());
      cacheManager.defineConfiguration(remoteCacheName, HotRodTestingUtil.hotRodCacheConfiguration().build());
      cacheManager.defineConfiguration(remoteBackupCacheName, HotRodTestingUtil.hotRodCacheConfiguration().build());
      cacheManager.defineConfiguration(customCacheName, HotRodTestingUtil.hotRodCacheConfiguration().build());
      HotRodServerConfigurationBuilder hcb = new HotRodServerConfigurationBuilder();
      hcb.port(15233);
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager, hcb);
   }

   @Override
   public void destroy() throws Exception {
      cacheManager.stop();
      hotrodServer.stop();
   }
}
