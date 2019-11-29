package org.infinispan.jmx.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.CLASS)
@Documented
public @interface Parameter {

   /**
    * Operation parameter name.
    */
   String name() default "";

   /**
    * Operation parameter description.
    */
   String description() default "";
}
