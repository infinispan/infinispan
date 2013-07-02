package org.infinispan.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Configuration bean element annotated with &#64;ConfigurationDocRef indicates that the element
 * should be included in configuration reference but that the description should be extracted from
 * bean class target and its targetElement (method or field).
 * 
 * 
 * @author Vladimir Blagojevic
 * @since 4.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD })
public @interface ConfigurationDocRef {
   String name() default "";

   Class<?> bean();

   String targetElement();
}
