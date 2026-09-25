package org.infinispan.query;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.query.impl.DefaultTransformer;

/**
 * Annotate objects to convert them to keys that Infinispan can index.
 * You need to annotate custom types only.
 *
 * @author Manik Surtani
 * @since 4.0
 * @deprecated Use {@link org.infinispan.api.annotations.query.Transformable} instead
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Deprecated
public @interface Transformable {

   /**
    * The {@link Transformer} to use. Please specify your custom transformer instead of relying on the default one which
    * is slow.
    */
   Class<? extends Transformer> transformer() default DefaultTransformer.class;
}
