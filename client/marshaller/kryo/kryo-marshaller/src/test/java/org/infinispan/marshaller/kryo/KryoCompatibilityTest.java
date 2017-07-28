package org.infinispan.marshaller.kryo;

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
@Test(groups = "functional", testName = "marshaller.kryo.KryoCompatibilityTest")
public class KryoCompatibilityTest extends AbstractCompatibilityTest {

   @BeforeClass
   protected void setup() throws Exception {
      KryoMarshaller marshaller = new KryoMarshaller();
      Transcoder transcoder = new KryoTranscoder(marshaller);
      cacheFactory = new CompatibilityCacheFactory<>("KryoCache", marshaller, CacheMode.LOCAL, transcoder);
      cacheFactory.setup();
      cacheFactory.registerEncoder(new KryoEncoder());
   }

   @Override
   protected Class<? extends Encoder> getEncoderClass() {
      return KryoEncoder.class;
   }
}
