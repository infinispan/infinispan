package org.infinispan.marshaller.protostuff;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.it.compatibility.CompatibilityCacheFactory;
import org.infinispan.marshaller.test.AbstractCompatibilityTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import net.spy.memcached.transcoders.Transcoder;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Test(groups = "functional", testName = "marshaller.protostuff.ProtostuffCompatibilityTest")
public class ProtostuffCompatibilityTest extends AbstractCompatibilityTest {
   @BeforeClass
   protected void setup() throws Exception {
      ProtostuffMarshaller marshaller = new ProtostuffMarshaller();
      Transcoder transcoder = new ProtostuffTranscoder(marshaller);
      cacheFactory = new CompatibilityCacheFactory<>("protoCache", marshaller, CacheMode.LOCAL, transcoder);
      cacheFactory.setup();
      cacheFactory.registerEncoder(new ProtostuffEncoder());
   }

   @Override
   protected Class<? extends Encoder> getEncoderClass() {
      return ProtostuffEncoder.class;
   }
}
