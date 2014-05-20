package org.infinispan.it.osgi;

import static org.infinispan.it.osgi.util.IspnKarafOptions.allOptions;

import org.infinispan.test.MultipleCacheManagersTest;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;

/**
 * @author mgencur
 * @author isavin
 */
public abstract class BaseInfinispanCoreOSGiTest extends MultipleCacheManagersTest {

   @Configuration
   public Option[] config() throws Exception {
      return allOptions();
   }

   @ProbeBuilder
   public TestProbeBuilder exportTestPackages(TestProbeBuilder probeBuilder) {
       StringBuilder builder = new StringBuilder();

       /* Export all test subpackages. */
       Package[] pkgs = Package.getPackages();
       for (Package pkg : pkgs) {
           String pkgName = pkg.getName();
           if (pkgName.startsWith("org.infinispan.it.osgi")) {
               if (builder.length() > 0) {
                   builder.append(",");
               }
               builder.append(pkgName);
           }
       }

       probeBuilder.setHeader("Export-Package", builder.toString());
       return probeBuilder;
   }
}
