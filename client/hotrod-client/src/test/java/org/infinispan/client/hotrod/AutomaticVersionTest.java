package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Test(testName = "client.hotrod.AutomaticVersionTest", groups = {"unit", "functional"})
public class AutomaticVersionTest {
   public void testBestProtocolSelection() {
      // This should choose the highest possible version known by the client
      assertEquals(ProtocolVersion.HIGHEST_PROTOCOL_VERSION, ProtocolVersion.getBestVersion(Integer.MAX_VALUE));

      // This should match the exact version, as we know about it
      assertEquals(ProtocolVersion.PROTOCOL_VERSION_23, ProtocolVersion.getBestVersion(23));
   }
}
