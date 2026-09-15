package org.infinispan.core.test.jupiter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field for injection of an {@link InfinispanContext}.
 * <p>
 * The annotated field must be of type {@link InfinispanContext} and can be
 * either static (shared across all tests) or instance-level (per-test).
 *
 * @since 16.2
 */
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface InfinispanResource {
}
