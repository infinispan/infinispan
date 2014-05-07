package org.infinispan.client.hotrod.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Galder Zamarreño
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ClientListener {
   String filterFactoryName() default "";
   String converterFactoryName() default "";
}
