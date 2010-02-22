package org.infinispan.config;

import org.infinispan.Cache;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
            "<infinispan\n" +
            "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            "      xsi:schemaLocation=\"urn:infinispan:config:4.0 http://www.infinispan.org/schemas/infinispan-config-4.0.xsd\"\n" +
            "      xmlns=\"urn:infinispan:config:4.0\">" +
            "<default><customInterceptors> \n" +
            "<interceptor after=\""+ InvocationContextInterceptor.class.getName()+"\" class=\""+DummyInterceptor.class.getName()+"\"/> \n" +
            "</customInterceptors> </default></infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      cm = TestCacheManagerFactory.fromStream(stream);
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


