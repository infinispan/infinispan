package org.infinispan.integrationtests.cdi.weld;

import static org.testng.Assert.assertEquals;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.testng.annotations.Test;

/**
 * Tests Weld integration in standalone (desktop) app.
 *
 * @author Sebastian Laskawiec
 */
@Test(groups="functional", testName="cdi.test.weld.WeldStandaloneTest")
public class WeldStandaloneTest {

   public void testWeldStandaloneInitialisation() {
      WeldContainer weld = null;
      try {
         //given
         weld = new Weld().initialize();
         CDITestingBean testedBean = weld.instance().select(CDITestingBean.class).get();

         //when
         testedBean.putValueInCache("test", "abcd");
         String retrievedValue = testedBean.getValueFromCache("test");

         //then
         assertEquals(retrievedValue, "abcd");
      } finally {
         if(weld != null) {
            //cleanup
            weld.shutdown();
         }
      }
   }

}
