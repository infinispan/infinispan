package org.infinispan.jmx.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a method in an MBean class defines an MBean operation. @ManagedOperation annotation can be applied to
 * a public method of a public class that is itself optionally annotated with an @MBean annotation, or inherits such an
 * annotation from a superclass.
 *
 * @author (various)
 * @since 4.0
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
@Documented
public @interface ManagedOperation {

   /**
    * The name of the JMX operation.
    */
   String name() default "";

   /**
    * The human-readable description of the operation.
    */
   String description() default "";

   /**
    * Similar with {@link #description} but shorter.
    */
   String displayName() default "";
}
