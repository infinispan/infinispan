package org.infinispan.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Configuration bean element annotated with &#64;ConfigurationDocs wraps multiple
 * &#64;ConfigurationDoc annotations
 * 
 * 
 * @author Vladimir Blagojevic
 * @since 4.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD, ElementType.TYPE })
public @interface ConfigurationDocs {
   ConfigurationDoc[] value();
}
