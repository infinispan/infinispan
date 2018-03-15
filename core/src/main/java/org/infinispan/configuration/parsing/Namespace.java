package org.infinispan.configuration.parsing;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Namespace. An annotation which allows specifying the namespace recognized by an implementation of
 * a {@link ConfigurationParser}. If you need to specify multiple namespaces, use the
 * {@link Namespaces} annotation.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Namespaces.class)
public @interface Namespace {
   /**
    * The URI of the namespace. Defaults to the empty string.
    */
   String uri() default "";

   /**
    * The root element of this namespace.
    */
   String root();

   /**
    * The first version of the schema where this is supported. Defaults to 7.0. Only considered if {@link #uri()} ends
    * with a wildcard
    */
   String since() default "7.0";

   /**
    * The last version of the schema where this is supported. Defaults to the current release. Only considered if {@link #uri()} ends
    * with a wildcard
    */
   String until() default "";
}
