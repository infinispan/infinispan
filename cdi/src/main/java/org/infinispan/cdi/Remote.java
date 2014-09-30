package org.infinispan.cdi;

import javax.enterprise.util.Nonbinding;
import javax.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Qualifier used to specify which remote cache will be injected.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Target({METHOD, FIELD, PARAMETER, TYPE})
@Retention(RUNTIME)
@Documented
@Qualifier
public @interface Remote {
   /**
    * The remote cache name. If no value is provided the default cache is assumed.
    */
   @Nonbinding String value() default "";
}
