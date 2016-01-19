package org.infinispan.client.hotrod;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "client.hotrod.ApacheCommonsPoolTest")
public class ApacheCommonsPoolTest {

   public void testBorrowValidObjectFromPool() throws Exception {
      KeyedObjectPool<Integer, String> pool =
            BasicPoolFactory.createPoolFactory().createPool();
      String obj = pool.borrowObject(1);
      assertEquals("1", obj);
   }

   @Test(expectedExceptions = TooHighException.class)
   public void testBorrowFromPoolException() throws Exception {
      GenericKeyedObjectPoolFactory<Integer, String> poolFactory =
            BasicPoolFactory.createPoolFactory();
      try {
         poolFactory.createPool().borrowObject(Integer.MAX_VALUE);
      } finally {
         BasicPoolFactory basicPoolFactory =
               (BasicPoolFactory) poolFactory.getFactory();
         assertEquals("invalid", basicPoolFactory.getState(Integer.MAX_VALUE));
      }
   }

   public void testInvalidateBorrowFromPool() throws Exception {
      GenericKeyedObjectPoolFactory<Integer, String> poolFactory =
            BasicPoolFactory.createPoolFactory();
      KeyedObjectPool<Integer, String> pool = poolFactory.createPool();
      try {
         pool.borrowObject(Integer.MAX_VALUE);
         fail("Should have thrown a TooHighException");
      } catch (TooHighException e) {
         // Expected, now invalidate object
         pool.invalidateObject(Integer.MAX_VALUE, null);
         BasicPoolFactory basicPoolFactory =
               (BasicPoolFactory) poolFactory.getFactory();
         assertEquals("destroyed", basicPoolFactory.getState(Integer.MAX_VALUE));
      }
   }

   private static class BasicPoolFactory
         extends BaseKeyedPoolableObjectFactory<Integer, String> {

      private Map<Integer, String> state = new HashMap<Integer, String>();

      private BasicPoolFactory() {
         // Singleton
      }

      @Override
      public String makeObject(Integer key) throws Exception {
         if (Integer.MAX_VALUE == key.intValue()) {
            state.put(key, "invalid");
            throw new TooHighException("Too high");
         }

         return key.toString();
      }

      @Override
      public void destroyObject(Integer key, String obj) throws Exception {
         state.put(key, "destroyed");
      }

      public String getState(Integer key) {
         return state.get(key);
      }

      public static GenericKeyedObjectPoolFactory<Integer, String> createPoolFactory() {
         return new GenericKeyedObjectPoolFactory<Integer, String>(
               new BasicPoolFactory());
      }

   }

   private static class TooHighException extends RuntimeException {

      public TooHighException(String message) {
         super(message);    // TODO: Customise this generated block
      }

   }

}
