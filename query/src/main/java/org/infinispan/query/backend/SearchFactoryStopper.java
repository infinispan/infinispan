package org.infinispan.query.backend;

import org.hibernate.search.engine.SearchFactoryImplementor;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * A helper component to clean up a search factory
 *
 * @author Manik Surtani
 * @version 4.2
 */
@Scope(Scopes.NAMED_CACHE)
public class SearchFactoryStopper {
   private final SearchFactoryImplementor searchFactory;

   public SearchFactoryStopper(SearchFactoryImplementor searchFactory) {
      this.searchFactory = searchFactory;
   }

   @Stop(priority = 1)
   public void cleanup() {
      searchFactory.close();
   }
}
