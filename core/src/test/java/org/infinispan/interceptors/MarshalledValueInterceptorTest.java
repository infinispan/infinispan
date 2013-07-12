package org.infinispan.interceptors;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "functional", testName = "interceptors.MarshalledValueInterceptorTest")
public class MarshalledValueInterceptorTest extends AbstractInfinispanTest {
   EmbeddedCacheManager cm;

   @BeforeTest
   public void setUp() {
      cm = TestCacheManagerFactory.createCacheManager(false);
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testDefaultInterceptorStack() {
      assert TestingUtil.findInterceptor(cm.getCache(), MarshalledValueInterceptor.class) == null;

      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.storeAsBinary().enable();
      cm.defineConfiguration("someCache", configuration.build());
      Cache<?, ?> c = cm.getCache("someCache");

      assert TestingUtil.findInterceptor(c, MarshalledValueInterceptor.class) != null;
      TestingUtil.killCaches(c);
   }

   public void testDisabledInterceptorStack() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.storeAsBinary().disable();
      cm.defineConfiguration("a", cfg.build());
      Cache<?, ?> c = cm.getCache("a");
      assert TestingUtil.findInterceptor(c, MarshalledValueInterceptor.class) == null;
   }

   public void testExcludedTypes() {
      // Strings
      assert MarshalledValue.isTypeExcluded(String.class);
      assert MarshalledValue.isTypeExcluded(String[].class);
      assert MarshalledValue.isTypeExcluded(String[][].class);
      assert MarshalledValue.isTypeExcluded(String[][][].class);

      // primitives
      assert MarshalledValue.isTypeExcluded(void.class);
      assert MarshalledValue.isTypeExcluded(boolean.class);
      assert MarshalledValue.isTypeExcluded(char.class);
      assert MarshalledValue.isTypeExcluded(byte.class);
      assert MarshalledValue.isTypeExcluded(short.class);
      assert MarshalledValue.isTypeExcluded(int.class);
      assert MarshalledValue.isTypeExcluded(long.class);
      assert MarshalledValue.isTypeExcluded(float.class);
      assert MarshalledValue.isTypeExcluded(double.class);

      assert MarshalledValue.isTypeExcluded(boolean[].class);
      assert MarshalledValue.isTypeExcluded(char[].class);
      assert MarshalledValue.isTypeExcluded(byte[].class);
      assert MarshalledValue.isTypeExcluded(short[].class);
      assert MarshalledValue.isTypeExcluded(int[].class);
      assert MarshalledValue.isTypeExcluded(long[].class);
      assert MarshalledValue.isTypeExcluded(float[].class);
      assert MarshalledValue.isTypeExcluded(double[].class);

      assert MarshalledValue.isTypeExcluded(boolean[][].class);
      assert MarshalledValue.isTypeExcluded(char[][].class);
      assert MarshalledValue.isTypeExcluded(byte[][].class);
      assert MarshalledValue.isTypeExcluded(short[][].class);
      assert MarshalledValue.isTypeExcluded(int[][].class);
      assert MarshalledValue.isTypeExcluded(long[][].class);
      assert MarshalledValue.isTypeExcluded(float[][].class);
      assert MarshalledValue.isTypeExcluded(double[][].class);

      assert MarshalledValue.isTypeExcluded(Void.class);
      assert MarshalledValue.isTypeExcluded(Boolean.class);
      assert MarshalledValue.isTypeExcluded(Character.class);
      assert MarshalledValue.isTypeExcluded(Byte.class);
      assert MarshalledValue.isTypeExcluded(Short.class);
      assert MarshalledValue.isTypeExcluded(Integer.class);
      assert MarshalledValue.isTypeExcluded(Long.class);
      assert MarshalledValue.isTypeExcluded(Float.class);
      assert MarshalledValue.isTypeExcluded(Double.class);

      assert MarshalledValue.isTypeExcluded(Boolean[].class);
      assert MarshalledValue.isTypeExcluded(Character[].class);
      assert MarshalledValue.isTypeExcluded(Byte[].class);
      assert MarshalledValue.isTypeExcluded(Short[].class);
      assert MarshalledValue.isTypeExcluded(Integer[].class);
      assert MarshalledValue.isTypeExcluded(Long[].class);
      assert MarshalledValue.isTypeExcluded(Float[].class);
      assert MarshalledValue.isTypeExcluded(Double[].class);

      assert MarshalledValue.isTypeExcluded(Boolean[][].class);
      assert MarshalledValue.isTypeExcluded(Character[][].class);
      assert MarshalledValue.isTypeExcluded(Byte[][].class);
      assert MarshalledValue.isTypeExcluded(Short[][].class);
      assert MarshalledValue.isTypeExcluded(Integer[][].class);
      assert MarshalledValue.isTypeExcluded(Long[][].class);
      assert MarshalledValue.isTypeExcluded(Float[][].class);
      assert MarshalledValue.isTypeExcluded(Double[][].class);
   }

   public void testNonExcludedTypes() {
      assert !MarshalledValue.isTypeExcluded(Object.class);
      assert !MarshalledValue.isTypeExcluded(List.class);
      assert !MarshalledValue.isTypeExcluded(Collection.class);
      assert !MarshalledValue.isTypeExcluded(Map.class);
      assert !MarshalledValue.isTypeExcluded(Date.class);
      assert !MarshalledValue.isTypeExcluded(Thread.class);
      assert !MarshalledValue.isTypeExcluded(Collection.class);
      assert !MarshalledValue.isTypeExcluded(new Object() {
         String blah;
      }.getClass());
   }
}
