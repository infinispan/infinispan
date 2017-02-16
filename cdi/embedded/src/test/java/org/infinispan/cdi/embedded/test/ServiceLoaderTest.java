package org.infinispan.cdi.embedded.test;

import static org.testng.AssertJUnit.fail;

import java.util.ServiceLoader;

import javax.enterprise.inject.spi.Extension;

import org.infinispan.cdi.embedded.InfinispanExtensionEmbedded;
import org.testng.annotations.Test;

@Test(groups="functional", testName="cdi.test.ServiceLoaderTest")
public class ServiceLoaderTest {

   public void testServiceLoaderExtension() {
      ServiceLoader<Extension> extensions = ServiceLoader.load(Extension.class);

      for(Extension extension : extensions) {
         if (extension instanceof InfinispanExtensionEmbedded)
            return;
      }
      fail("Could not load Infinispan CDI Extension");
   }


}
