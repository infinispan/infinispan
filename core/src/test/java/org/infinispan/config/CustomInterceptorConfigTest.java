package org.infinispan.config;

import org.infinispan.Cache;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;

import static org.infinispan.test.TestingUtil.withCacheManager;

@Test(testName = "config.CustomInterceptorConfigTest", groups = "functional")
public class CustomInterceptorConfigTest extends AbstractInfinispanTest {

   public void testCustomInterceptors() throws Exception {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<infinispan>" +
            "<default><customInterceptors> \n" +
            "<interceptor after=\""+ InvocationContextInterceptor.class.getName()+"\" class=\""+DummyInterceptor.class.getName()+"\"/> \n" +
            "</customInterceptors> </default>" +
            "<namedCache name=\"x\">" +
            "<customInterceptors>\n" +
            "         <interceptor position=\"first\" class=\""+CustomInterceptor1.class.getName()+"\" />" +
            "         <interceptor" +
            "            position=\"last\"" +
            "            class=\""+CustomInterceptor2.class.getName()+"\"" +
            "         />" +
            "</customInterceptors>" +
            "</namedCache>" +
            "</infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(stream)){
         @Override
         public void call() {
            Cache c = cm.getCache();
            DummyInterceptor i = TestingUtil.findInterceptor(c, DummyInterceptor.class);
            assert i != null;

            Cache<Object, Object> namedCacheX = cm.getCache("x");
            assert TestingUtil.findInterceptor(namedCacheX, CustomInterceptor1.class) != null;
            assert TestingUtil.findInterceptor(namedCacheX, CustomInterceptor2.class) != null;
         }
      });
   }
   
   public static final class CustomInterceptor1 extends CommandInterceptor {}
   public static final class CustomInterceptor2 extends CommandInterceptor {}


   public void testCustomInterceptorsProgramatically() {
      Configuration cfg = new Configuration();
      cfg.setLockAcquisitionTimeout(1010);
      CustomInterceptorConfig cic = new CustomInterceptorConfig(new DummyInterceptor(), true, false, -1, "", "");
      cfg.setCustomInterceptors(Collections.singletonList(cic));
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(cfg)) {
         @Override
         public void call() {
            Cache c = cm.getCache();
            DummyInterceptor i = TestingUtil.findInterceptor(c, DummyInterceptor.class);
            assert i != null;
         }
      });
   }

   public void testCustomInterceptorsProgramaticallyWithOverride() {
      final Configuration cfg = new Configuration();
      cfg.setLockAcquisitionTimeout(1010);
      CustomInterceptorConfig cic = new CustomInterceptorConfig(new DummyInterceptor(), true, false, -1, "", "");
      cfg.setCustomInterceptors(Collections.singletonList(cic));
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(new Configuration())) {
         @Override
         public void call() {
            cm.defineConfiguration("custom", cfg);
            Cache c = cm.getCache("custom");
            DummyInterceptor i = TestingUtil.findInterceptor(c, DummyInterceptor.class);
            assert i != null;
         }
      });

   }

   public static class DummyInterceptor extends CommandInterceptor {

   }
}


