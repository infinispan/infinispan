package org.infinispan.query.backend;

import org.testng.annotations.Test;

/**
 * @author Navin Surtani
 */

@Test
public class TransactionalEventTransactionContextTest
{

   @Test (expectedExceptions = NullPointerException.class)
   public void testNullConstuctor()
   {
      TransactionalEventTransactionContext tetc = new TransactionalEventTransactionContext(null);
   }



}
