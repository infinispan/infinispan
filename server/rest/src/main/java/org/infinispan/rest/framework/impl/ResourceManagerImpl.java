package org.infinispan.rest.framework.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.ResourceManager;

/**
 * @since 10.0
 */
public class ResourceManagerImpl implements ResourceManager {

   private final ResourceNode resourceTree;
   private final String rootPath;

   public ResourceManagerImpl(String rootPath) {
      this.rootPath = rootPath;
      this.resourceTree = new ResourceNode(new StringPathItem(rootPath), null);
   }


   @Override
   public void registerResource(ResourceHandler handler) {
      handler.getInvocations().forEach(invocation -> {
         Set<String> paths = invocation.paths();
         paths.stream().map(this::removeLeadSlash).forEach(path -> {
            List<PathItem> p = Arrays.stream(path.split("/")).map(PathItem::fromString).collect(Collectors.toList());
            resourceTree.insertPath(invocation, p);
         });
      });
   }

   private String removeLeadSlash(String path) {
      if (path.startsWith("/")) return path.substring(1);
      return path;
   }

   @Override
   public LookupResult lookupResource(Method method, String path, String action) {
      List<PathItem> pathItems = Arrays.stream(removeLeadSlash(path).split("/"))
            .map(PathItem::fromString).collect(Collectors.toList());
      PathItem startPath = pathItems.iterator().next();
      if (!"*".equals(rootPath) && !rootPath.equals(startPath.getPath())) return null;

      return resourceTree.find(method, pathItems.subList(1, pathItems.size()), action);
   }

}
