package org.infinispan.cdi.test;

import java.util.ServiceLoader;

import javax.enterprise.inject.spi.Extension;

import org.infinispan.cdi.InfinispanExtension;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

@Test(groups="functional", testName="cdi.test.ServiceLoaderTest")
public class ServiceLoaderTest {

   public void testServiceLoaderExtension() {
      ServiceLoader<Extension> extensions = ServiceLoader.load(Extension.class);

      for(Extension extension : extensions) {
         if (extension instanceof InfinispanExtension)
            return;
      }
      fail("Could not load Infinispan CDI Extension");
   }


}
