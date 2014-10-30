package org.infinispan.server.infinispan.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;

/**
 * GetSearchManagerAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetSearchManagerAction extends AbstractAdvancedCacheAction<SearchManager> {

    public GetSearchManagerAction(AdvancedCache<?, ?> cache) {
        super(cache);
    }

    @Override
    public SearchManager run() {
        return Search.getSearchManager(cache);
    }

}
