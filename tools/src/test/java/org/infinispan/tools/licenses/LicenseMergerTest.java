package org.infinispan.tools.licenses;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Test(testName = "org.infinispan.tools.licenses.LicenseMergerTest", groups = "functional")
public class LicenseMergerTest {

   public void testMergeLicenses() throws Exception {
      LicenseMerger merger = new LicenseMerger();
      merger.loadLicense(getClass().getClassLoader().getResource("licenses/artifact1-1.0.0.xml").getFile());
      merger.loadLicense(getClass().getClassLoader().getResource("licenses/artifact2-1.0.0.xml").getFile());

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      merger.write(false, baos);
      String out = baos.toString(StandardCharsets.UTF_8.name());
      assertTrue(out.contains("artifact1"));
      assertTrue(out.contains("artifact2"));
      assertTrue(out.contains("artifact3"));
   }

   public void testMergeLicensesInclusive() throws Exception {
      LicenseMerger merger = new LicenseMerger();
      merger.loadLicense(getClass().getClassLoader().getResource("licenses/artifact1-1.0.0.xml").getFile());
      merger.loadLicense(getClass().getClassLoader().getResource("licenses/artifact2-1.0.0.xml").getFile());

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      merger.write(true, baos);
      String out = baos.toString(StandardCharsets.UTF_8.name());
      assertTrue(out.contains("artifact1"));
      assertTrue(out.contains("artifact2"));
      assertFalse(out.contains("artifact3"));
   }
}
