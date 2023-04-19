package org.infinispan.commons.graalvm;

import java.util.stream.Stream;

public interface NativeMetadataProvider {
   default Stream<ReflectiveClass> reflectiveClasses() {
      return Stream.empty();
   }

   default Stream<Resource> includedResources() {
      return Stream.empty();
   }
   default Stream<Bundle> bundles() {
      return Stream.empty();
   }
}
