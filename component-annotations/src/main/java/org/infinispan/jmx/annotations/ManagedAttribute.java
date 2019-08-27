package org.infinispan.jmx.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a public method or a field (any visibility) in an MBean class defines an MBean attribute. This
 * annotation can be applied to either a field or a public setter and/or getter method of a public class that is itself
 * annotated with an @MBean annotation, or inherits such an annotation from a superclass.
 *
 * @author (various)
 * @author Galder Zamarreño
 * @since 4.0
 */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface ManagedAttribute {

   /**
    * The human-readable description of the attribute. Probably more detailed than {@link #displayName}.
    */
   String description() default "";

   /**
    * Indicates if the attribute is writable or just read-only (default).
    */
   boolean writable() default false;

   /**
    * A brief and user friendly name.
    */
   String displayName() default "";

   DataType dataType() default DataType.MEASUREMENT;

   MeasurementType measurementType() default MeasurementType.DYNAMIC;

   Units units() default Units.NONE;
}
