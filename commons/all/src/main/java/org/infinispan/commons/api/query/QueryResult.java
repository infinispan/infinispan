package org.infinispan.commons.api.query;

import java.util.List;

public interface QueryResult<T> {

   /**
    * @return An object containing information about the number of hits from the query, ignoring pagination.
    */
   HitCount count();

   /**
    * @return The results of the query as a List, respecting the bounds specified in {@link Query#startOffset(long)} and
    * {@link Query#maxResults(int)}. This never returns {@code null} but will always be an empty List for {@link Query#executeStatement}.
    */
   List<T> list();

}
