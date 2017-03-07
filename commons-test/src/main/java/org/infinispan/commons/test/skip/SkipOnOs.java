package org.infinispan.commons.test.skip;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows to skip a test on certain Operation Systems.
 *
 * <p>
 *    Note that TestNG implementation does not work properly on inherited test methods. In other words, you always
 *    need to override all <code>test*</code> methods in a class annotated using <code>SkipOnOs</code>
 * </p>
 *
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
