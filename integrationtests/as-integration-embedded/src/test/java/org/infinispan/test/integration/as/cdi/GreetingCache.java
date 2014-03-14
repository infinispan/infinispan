package org.infinispan.test.integration.as.cdi;

import javax.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>The greeting cache qualifier.</p>
 *
 * <p>This qualifier will be associated to the greeting cache in the {@link Config} class.</p>
 *
 * @author Kevin Pollet <pollet.kevin@gmail.com> (C) 2011
 */
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Qualifier
@Documented
public @interface GreetingCache {
}
