package org.infinispan.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Configuration bean element annotated with &#64;ConfigurationDoc indicates that the element should
 * be included in configuration reference.
 * 
 * 
 * @author Vladimir Blagojevic
 * @since 4.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD, ElementType.TYPE })
public @interface ConfigurationDoc {
   String name() default "";

   String parentName() default "";

   String desc() default "";
}
