package org.infinispan.cdi.embedded;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is used to define a cache {@link Configuration}.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Retention(RUNTIME)
@Target({METHOD, FIELD, PARAMETER, TYPE})
public @interface ConfigureCache {
   /**
    * The name of the cache to configure. If no value is provided the configured cache is the default one.
    */
   String value() default "";
}
