package org.infinispan.compatibility.adaptor52x;

import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test(groups = "functional", testName = "compatibility.adaptor52x.Adaptor52xCustomStoreTest")
public class Adaptor52xCustomStoreTest extends Adaptor52xCustomLoaderTest {

   public Adaptor52xCustomStoreTest() {
      configurationFile = "52x-custom-store.xml";
   }
}
