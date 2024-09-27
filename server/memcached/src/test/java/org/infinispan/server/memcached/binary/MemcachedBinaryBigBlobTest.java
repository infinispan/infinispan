package org.infinispan.server.memcached.binary;

import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.test.MemcachedBigBlobTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.memcached.binary.MemcachedBinaryBigBlobTest")
public class MemcachedBinaryBigBlobTest extends MemcachedBigBlobTest {

   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.BINARY;
   }
}
