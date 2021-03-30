package org.infinispan.cli.resources;

import java.io.IOException;
import java.util.Optional;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CacheResource extends AbstractResource {
   public CacheResource(CachesResource parent, String name) {
      super(parent, name);
   }

   @Override
   public Iterable<String> getChildrenNames() throws IOException {
      return getConnection().getCacheKeys(getParent().getParent().getName(), name);
   }

   @Override
   public boolean isLeaf() {
      return true;
   }

   @Override
   public Resource getChild(String name) {
      if (Resource.PARENT.equals(name)) {
         return parent;
      } else {
         return new CacheKeyResource(this, name);
      }
   }

   @Override
   public String describe() throws IOException {
      return getConnection().describeCache(getParent().getParent().getName(), name);
   }

   public static String cacheName(Resource resource) {
      return resource.findAncestor(CacheResource.class).getName();
   }

   public static Optional<String> findCacheName(Resource resource) {
      return resource.optionalFindAncestor(CacheResource.class).map(AbstractResource::getName);
   }
}
