package org.infinispan.integrationtests.cdi.weld;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Tests Weld integration in standalone (desktop) app.
 *
 * @author Sebastian Laskawiec
 */
@Test(groups="functional", testName="cdi.test.weld.WeldStandaloneTest")
public class WeldStandaloneTest {

   public void testWeldStandaloneInitialisation() throws Exception {
      //given
      WeldContainer weld = new Weld().initialize();
      CDITestingBean testedBean = weld.instance().select(CDITestingBean.class).get();

      //when
      testedBean.putValueInCache("test", "abcd");
      String retrievedValue = testedBean.getValueFromCache("test");

      //then
      assertEquals(retrievedValue, "abcd");
   }

}
