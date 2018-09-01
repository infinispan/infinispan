package org.infinispan.query;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.query.impl.DefaultTransformer;

/**
 * If you annotate your object with this, it can be used as a valid key for Infinispan to index (unless your key type is
 * a boxed primitive, a String or a byte array in which case it can be used without a transformer).
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Transformable {

   /**
    * The {@link Transformer} to use. Please specify your custom transformer instead of relying on the default one which
    * is slow.
    */
   Class<? extends Transformer> transformer() default DefaultTransformer.class;
}
