package org.infinispan.tasks.query;

import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.util.Experimental;

@Experimental
public interface RemoteQueryAccess {

   <T> Query<T> query(String query);

}
