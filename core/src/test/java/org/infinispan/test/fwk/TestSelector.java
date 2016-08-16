package org.infinispan.test.fwk;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Predicate;

import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;

/**
 * See {@link ChainMethodInterceptor}. Works in a similar way as annotating test class
 * with {@link org.testng.annotations.Listeners} with {@link IMethodInterceptor} but allows
 * multiple interceptors and multiple filters.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TestSelector {
   /**
    * Filters are applied before the interceptors to remove unwanted methods.
    */
   Class<? extends Predicate<IMethodInstance>>[] filters() default {};

   /**
    * Interceptors are applied later to e.g. sort methods.
    */
   Class<? extends IMethodInterceptor>[] interceptors() default {};
}
