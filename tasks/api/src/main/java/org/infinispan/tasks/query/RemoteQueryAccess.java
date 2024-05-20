package org.infinispan.tasks.query;

import java.io.IOException;
import java.util.Map;

import org.infinispan.commons.api.query.QueryResult;

public interface RemoteQueryAccess {

   QueryResult<?> executeQuery(String queryString, Map<String, Object> namedParametersMap, Integer offset,
                               Integer maxResults, Integer hitCountAccuracy, boolean isLocal) throws IOException;

}
