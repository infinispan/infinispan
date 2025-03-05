package org.infinispan.spring.remote.provider.sample;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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
 * @author Olaf Bergner
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
      ConfigurationBuilder builder = HotRodTestingUtil.hotRodCacheConfiguration(APPLICATION_SERIALIZED_OBJECT);
      cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      Configuration configuration = builder.build();
      cacheManager.defineConfiguration(remoteCacheName, configuration);
      cacheManager.defineConfiguration(remoteBackupCacheName, configuration);
      cacheManager.defineConfiguration(customCacheName, configuration);
      HotRodServerConfigurationBuilder hcb = new HotRodServerConfigurationBuilder();
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager, 15233, hcb);
   }

   @Override
   public void destroy() {
      cacheManager.stop();
      hotrodServer.stop();
   }
}
