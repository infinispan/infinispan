package org.infinispan.commons.test.skip;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows to skip a test on certain Operation Systems.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Inherited
public @interface SkipOnOs {

   enum OS {
      UNIX, WINDOWS, SOLARIS
   }

   /**
    * @return A list of Operation Systems on which this test should be skipped.
    */
   OS [] value();
}
