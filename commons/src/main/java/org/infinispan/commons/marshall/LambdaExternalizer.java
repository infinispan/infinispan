package org.infinispan.commons.marshall;

/**
 * A lambda {@link AdvancedExternalizer}.
 *
 * @param <T>
 * @since 8.0
 */
public interface LambdaExternalizer<T> extends AdvancedExternalizer<T> {

   ValueMatcherMode valueMatcher(Object o);

}
