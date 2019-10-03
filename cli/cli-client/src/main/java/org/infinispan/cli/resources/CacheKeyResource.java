package org.infinispan.cli.resources;

import java.io.IOException;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CacheKeyResource extends AbstractResource {
   public CacheKeyResource(CacheResource parent, String name) {
      super(parent, name);
   }

   @Override
   public boolean isLeaf() {
      return true;
   }

   @Override
   public String describe() throws IOException {
      return getConnection().describeKey(getParent().getParent().getName(), getParent().getName(), name);
   }
}
