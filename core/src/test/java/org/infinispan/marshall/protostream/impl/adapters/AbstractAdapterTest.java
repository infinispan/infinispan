package org.infinispan.marshall.protostream.impl.adapters;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;

abstract class AbstractAdapterTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return new DefaultCacheManager();
   }

   @SuppressWarnings("unchecked")
   protected  <T> T deserialize(T object) throws Exception {
      Marshaller marshaller = TestingUtil.extractGlobalMarshaller(cacheManager);
      byte[] bytes = marshaller.objectToByteBuffer(object);
      return (T) marshaller.objectFromByteBuffer(bytes);
   }
}
