package org.infinispan.security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mark a method as secure.
 *
 * <p>
 * This annotation is utilized only by testing to guarantee the methods are overridden and properly verified. A method
 * with this annotation is skipped during verification since it is deemed secure already.
 * </p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SecureMethod { }
