package org.infinispan.jmx;

import org.infinispan.jmx.annotations.ManagedOperation;
import org.testng.annotations.Test;

/**
 * /** Tester class for {@link ResourceDMBean}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "unit", testName = "jmx.ResourceDMBeanTest")
public class ResourceDMBeanTest {

   /**
    * If we have a method in the base class that is annotated as @ManagedOperation, will this be seen the same way in
    * the inherited class?
    */
   public void testInheritedMethod() {
      Bbb bbb = new Bbb();
      ResourceDMBean resourceDMBean = new ResourceDMBean(bbb);
      assert resourceDMBean.isOperationRegistred("baseMethod");
   }

   static class Aaa {
      @ManagedOperation
      public void baseMethod() {
      }
   }

   static class Bbb extends Aaa {
      public void localMethod() {
      }
   }
}
