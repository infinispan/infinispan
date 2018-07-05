package org.infinispan.client.hotrod.impl.iteration;


import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;


/**
 * Remote iterator test with server storing java objects instead of binary content.
 *
 * @author vjuranek
 * @since 8.2
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.SingleServerObjectStorageRemoteIteratorTest")
public class SingleServerObjectStorageRemoteIteratorTest extends SingleServerRemoteIteratorTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = HotRodTestingUtil.hotRodCacheConfiguration();
      cb.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cb.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager(cb);
      cacheManager.getClassWhiteList().addClasses(AccountHS.class);
      return cacheManager;
   }

}
