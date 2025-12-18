package org.infinispan.rest.framework;

import java.util.List;
import java.util.function.BiConsumer;

public interface InvocationRegistry {

   void insertPath(Invocation invocation, List<PathItem> path);

   LookupResult find(Method method, List<PathItem> path, String action);

   void traverse(BiConsumer<PathItem, Invocation> consumer);
}
