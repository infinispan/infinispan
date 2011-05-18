package org.infinispan.distribution.group;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Identifies the key for a group.
 * 
 * @see Grouper
 * 
 * @author Pete
 *
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface Group {

}
