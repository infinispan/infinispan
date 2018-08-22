package org.infinispan.jmx.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Classes annotated with this will be exposed as MBeans. If you are looking for more fined grained way of exposing JMX
 * attributes/operations, take a look at {@link ManagedAttribute} and {@link
 * ManagedOperation}
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Inherited
public @interface MBean {
   String objectName() default "";

   String description() default "";
}
