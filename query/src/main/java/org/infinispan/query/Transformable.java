package org.infinispan.query;

import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.infinispan.query.impl.DefaultTransformer;

/**
 * If you annotate your object with this, it can be used as a valid key for Infinispan to index.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Transformable {

   // This should really be Class<? extends Transformer> however, since
   // migrating to JBoss Logging and using annotation processor for generating
   // log objects, if the previous definition is used, a compiler bug is hit.
   // You can find more info in https://issues.jboss.org/browse/ISPN-380
   Class transformer() default DefaultTransformer.class;
}
