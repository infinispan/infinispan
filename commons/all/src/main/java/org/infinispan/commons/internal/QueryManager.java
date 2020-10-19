package org.infinispan.commons.internal;

import org.infinispan.commons.api.Query;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@Scope(Scopes.NAMED_CACHE)
public interface QueryManager {

   <T> Query<T> createQuery(String queryString);
}
