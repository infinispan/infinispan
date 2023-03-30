package org.infinispan.server.memcached.text;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.test.MemcachedSingleNodeTest;
import org.testng.annotations.Test;

import net.spy.memcached.CASValue;

/**
 * @since 15.0
 **/
@Test(groups = "functional", testName = "server.memcached.text.MemcachedTextAuthenticationTest")
public class MemcachedTextAuthenticationTest extends MemcachedSingleNodeTest {
   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.TEXT;
   }

   @Override
   protected boolean withAuthentication() {
      return true;
   }

   public void testAuthentication(Method m) throws ExecutionException, InterruptedException, TimeoutException {
      String k = k(m);
      String v = v(m);
      wait(client.set(k, 0, v));
      CASValue<Object> v1 = client.gets(k);
      assertEquals(v, v1.getValue());
      wait(client.set(k, 0, v + "+"));
      CASValue<Object> v2 = client.gets(k);
      assertEquals(v + "+", v2.getValue());
      assertFalse(v1.getCas() == v2.getCas());
      wait(client.delete(k));
   }

}
