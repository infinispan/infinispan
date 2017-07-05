package org.infinispan.query.backend;

/**
 * Add extra methods.
 *
 * @author Ales Justin
 */
public interface ExtendedSearchWorkCreator extends SearchWorkCreator {
   boolean shouldRemove(SearchWorkCreatorContext context);
}
