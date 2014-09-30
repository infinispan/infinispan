package org.infinispan.cdi;

import javax.inject.Qualifier;

import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.mapreduce.MapReduceTask;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Qualifier indicating the injected Cache should be the input Cache used to create
 * {@link DefaultExecutorService} or {@link MapReduceTask}
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
