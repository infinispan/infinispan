package org.infinispan.config;

import org.infinispan.Cache;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

@Test(testName = "config.CustomInterceptorConfigTest", groups = "functional")
public class CustomInterceptorConfigTest {
   Cache c;
   CacheManager cm;

   public void testCustomInterceptors() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:infinispan:config:4.0\">" +
            "<default><customInterceptors> \n" +
            "<interceptor after=\""+ InvocationContextInterceptor.class.getName()+"\" class=\""+DummyInterceptor.class.getName()+"\"/> \n" +
            "</customInterceptors> </default></infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      cm = new DefaultCacheManager(stream);
      c = cm.getCache();
      DummyInterceptor i = TestingUtil.findInterceptor(c, DummyInterceptor.class);
      assert i != null;
   }

   @AfterMethod
   public void tearDown() {
      if (cm != null) cm.stop();
   }

   public static class DummyInterceptor extends CommandInterceptor {

   }
}


