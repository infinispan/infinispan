package org.infinispan.server.memcached.text;

import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.test.MemcachedBigBlobTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.memcached.text.MemcachedTextBigBlobTest")
public class MemcachedTextBigBlobTest extends MemcachedBigBlobTest {

   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.TEXT;
   }

}
