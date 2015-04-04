package org.infinispan.query.backend;

/**
 * Add extra methods.
 *
 * @author Ales Justin
 */
public interface ExtendedSearchWorkCreator<T> extends SearchWorkCreator<T> {
   boolean shouldRemove(SearchWorkCreatorContext context);
}
