package org.infinispan.commons.graalvm;

import java.nio.file.Path;
import java.util.List;

import org.graalvm.nativeimage.hosted.Feature;

public class ClassLoaderFeatureAccess implements Feature.FeatureAccess {

   private final ClassLoader cl;

   public ClassLoaderFeatureAccess(ClassLoader cl) {
      this.cl = cl;
   }

   @Override
   public Class<?> findClassByName(String className) {
      try {
         return Class.forName(className, false, cl);
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public List<Path> getApplicationClassPath() {
      throw new IllegalStateException();
   }

   @Override
   public List<Path> getApplicationModulePath() {
      throw new IllegalStateException();
   }

   @Override
   public ClassLoader getApplicationClassLoader() {
      return cl;
   }
}
