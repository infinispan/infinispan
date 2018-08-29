package org.infinispan.marshaller.protostuff;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.it.endpoints.EndpointsCacheFactory;
import org.infinispan.marshaller.test.AbstractInteropTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import net.spy.memcached.transcoders.Transcoder;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Test(groups = "functional", testName = "marshaller.protostuff.ProtostuffInteropTest")
public class ProtostuffInteropTest extends AbstractInteropTest {
   @BeforeClass
   protected void setup() throws Exception {
      ProtostuffMarshaller marshaller = new ProtostuffMarshaller();
      Transcoder transcoder = new ProtostuffTranscoder(marshaller);
      cacheFactory = new EndpointsCacheFactory<>("protoCache", marshaller, CacheMode.LOCAL, transcoder);
      cacheFactory.setup();
   }
}
