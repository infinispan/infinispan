package org.infinispan.commons.test.skip;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;

public class SkipOnOsListener implements IAnnotationTransformer {

   @Override
   public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod) {
      SkipOnOs annotationOnClass = testClass != null ? (SkipOnOs) testClass.getAnnotation(SkipOnOs.class) : null;
      SkipOnOs annotationOnMethod = testMethod != null ? testMethod.getAnnotation(SkipOnOs.class) : null;

      if(annotationOnMethod != null || annotationOnClass != null) {
         Set<SkipOnOs.OS> skipOnOs = new HashSet<>();
         if(annotationOnMethod != null) {
            skipOnOs.addAll(Arrays.asList(annotationOnMethod.value()));
         }
         if(annotationOnClass != null) {
            skipOnOs.addAll(Arrays.asList(annotationOnClass.value()));
         }
         if(skipOnOs.contains(SkipOnOsUtils.getOs())) {
            annotation.setEnabled(false);
            String msg = "Skipping " + (testMethod != null ? testMethod.getName() : testClass != null ? testClass.getName() : null) + " on " + skipOnOs;
            annotation.setDescription(msg);
            System.out.println(msg);
         }
      }
   }
}
