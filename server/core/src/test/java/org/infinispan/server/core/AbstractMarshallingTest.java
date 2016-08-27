package org.infinispan.server.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Random;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Abstract class to help marshalling tests in different server modules.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public abstract class AbstractMarshallingTest extends AbstractInfinispanTest {

   protected StreamingMarshaller marshaller;
   protected EmbeddedCacheManager cm;

   @BeforeClass(alwaysRun=true)
   public void setUp() {
      // Manual addition of externalizers to replication what happens in fully functional tests
      cm = TestCacheManagerFactory.createCacheManager();
      marshaller = TestingUtil.extractGlobalMarshaller(cm.getCache().getCacheManager());
   }

   @AfterClass(alwaysRun=true)
   public void tearDown() {
     if (cm != null) cm.stop();
   }

   protected byte[] getBigByteArray() {
      String value = new String(randomByteArray(1000));
      ByteArrayOutputStream result = new ByteArrayOutputStream(1000);
      try {
         ObjectOutputStream oos = new ObjectOutputStream(result);
         oos.writeObject(value);
         return result.toByteArray();
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }

   private byte[] randomByteArray(int i) {
      Random r = new Random();
      byte[] result = new byte[i];
      r.nextBytes(result);
      return result;
   }
}
