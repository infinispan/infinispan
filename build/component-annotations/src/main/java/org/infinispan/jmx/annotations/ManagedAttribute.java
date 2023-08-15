package org.infinispan.jmx.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a method in a MBean class defines a MBean attribute. This annotation can be applied to a non-static
 * non-private getter or setter method of a public class that is itself annotated with an @MBean annotation, or inherits
 * such an annotation from a superclass.
 *
 * @author (various)
 * @author Galder Zamarreño
 * @since 4.0
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
@Documented
public @interface ManagedAttribute {

   /**
    * The name of the JMX attribute. If left empty it defaults to the name of the Java property.
    */
   String name() default "";

   /**
    * The human-readable description of the attribute. Probably more detailed than {@link #displayName}.
    */
   String description() default "";

   /**
    * Indicates if the attribute is writable or just read-only (default). If this flag is true a setter method must
    * exist.
    */
   boolean writable() default false;

   /**
    * A brief and user friendly name.
    */
   String displayName() default "";

   DataType dataType() default DataType.MEASUREMENT;

   MeasurementType measurementType() default MeasurementType.DYNAMIC;

   Units units() default Units.NONE;

   /**
    * Indicates if the attribute relates to a cluster-wide attribute.
    */
   boolean clusterWide() default false;
}
