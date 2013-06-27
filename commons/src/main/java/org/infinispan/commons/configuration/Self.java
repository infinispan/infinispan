package org.infinispan.commons.configuration;

/**
 * This interface simplifies the task of writing fluent builders which need to inherit from
 * other builders (abstract or concrete). It overcomes Java's limitation of not being able to
 * return an instance of a class narrowed to the class itself. It should be used by all {@link Builder}
 * classes which require inheritance.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface Self<S extends Self<S>> {
   S self();
}
