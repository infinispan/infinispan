package org.infinispan.graalvm;

import org.infinispan.commons.graalvm.ReflectiveClass;

public class Feature implements org.graalvm.nativeimage.hosted.Feature {

   public void beforeAnalysis(org.graalvm.nativeimage.hosted.Feature.BeforeAnalysisAccess beforeAnalysis) {
      new NativeMetadataProvider(beforeAnalysis)
            .reflectiveClasses()
            .forEach(ReflectiveClass::register);
   }

   @Override
   public String getDescription() {
      return "Infinispan Embedded static reflection registrations for GraalVM";
   }
}
