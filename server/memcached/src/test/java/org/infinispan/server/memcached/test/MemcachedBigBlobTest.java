package org.infinispan.server.memcached.test;

import static org.infinispan.test.TestingUtil.generateRandomString;
import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.Test;

import net.spy.memcached.internal.OperationFuture;

@Test(groups = "functional", testName = "server.memcached.test.MemcachedBigBlobTest")
public abstract class MemcachedBigBlobTest extends MemcachedSingleNodeTest {

   {
      decoderReplay = false;
   }

   public void testSetBigSizeValue(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, generateRandomString(1024 * 1024).getBytes());
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
   }
}
