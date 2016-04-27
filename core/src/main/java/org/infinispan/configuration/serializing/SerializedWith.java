package org.infinispan.configuration.serializing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * SerializedWith, specifies the {@link ConfigurationSerializer} to use to serialize the annotated class
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SerializedWith {
   Class<? extends ConfigurationSerializer> value();
}
