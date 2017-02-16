package org.infinispan.cdi.embedded;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

import org.infinispan.distexec.DefaultExecutorService;

/**
 * Qualifier indicating the injected Cache should be the input Cache used to create
 * {@link DefaultExecutorService}
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@Target({ METHOD, FIELD, PARAMETER, TYPE })
@Retention(RUNTIME)
@Documented
@Qualifier
public @interface Input {

}
