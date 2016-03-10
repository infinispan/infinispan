package org.infinispan.cdi;

import org.infinispan.distexec.DefaultExecutorService;

import javax.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

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
