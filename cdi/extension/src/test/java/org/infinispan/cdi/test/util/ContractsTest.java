package org.infinispan.cdi.test.util;

import org.testng.annotations.Test;

import static org.infinispan.cdi.util.Contracts.assertNotNull;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "unit", testName = "cdi.test.util.ContractsTest")
public class ContractsTest {

   @Test(expectedExceptions = NullPointerException.class,
         expectedExceptionsMessageRegExp = "This parameter cannot be null")
   public void testAssertNotNullOnNullParameter() {
      assertNotNull(null, "This parameter cannot be null");
   }

   public void testAssertNotNullOnNotNullParameter() {
      assertNotNull("not null", "This parameter cannot be null");
   }
}
