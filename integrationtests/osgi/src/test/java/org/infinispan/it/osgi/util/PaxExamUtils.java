package org.infinispan.it.osgi.util;

import java.lang.reflect.Field;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.ops4j.pax.exam.ExamSystem;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.spi.reactors.ReactorManager;

public class PaxExamUtils {
   private static Log log = LogFactory.getLog(PaxExamUtils.class);

   /**
    *  Create a new probe, don't reuse the default.
    * 
    *  PAX EXAM reuses the default one which means all the test addresses
    *  from the previous runs are present in the probe header. If some of them
    *  extend classes from dependencies which are not available for the current
    *  probe bundle NoClassDefFound exceptions will occur.
    */
   public static TestProbeBuilder probeIsolationWorkaround(TestProbeBuilder probeBuilder) {
      ReactorManager reactorManager = ReactorManager.getInstance();
      try {
         
         Field fieldSystem = ReactorManager.class.getDeclaredField("system");
         fieldSystem.setAccessible(true);
         ExamSystem system = (ExamSystem) fieldSystem.get(reactorManager);
         return system.createProbe();
      } catch (Exception e) {
         log.error("Error creating test probe", e);
      }
      return probeBuilder;
   }

   public static TestProbeBuilder exportTestPackages(TestProbeBuilder probeBuilder) {
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
