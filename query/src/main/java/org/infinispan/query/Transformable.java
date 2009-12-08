package org.infinispan.query;

import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * If you annotate your object with this, it can be used as a valid key for Infinispan to index.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Transformable {
   Class<? extends Transformer> transformer() default DefaultTransformer.class;
}
