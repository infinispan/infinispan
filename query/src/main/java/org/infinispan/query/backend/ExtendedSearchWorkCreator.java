package org.infinispan.query.backend;

//todo [anistor] this class will be removed in Infinispan 10

/**
 * Add extra methods.
 *
 * @author Ales Justin
 * @deprecated without replacement
 */
@Deprecated
public interface ExtendedSearchWorkCreator extends SearchWorkCreator {
   boolean shouldRemove(SearchWorkCreatorContext context);
}
