package org.infinispan.atomic;

import org.infinispan.Cache;
import java.io.Externalizable;

/**
 *
 * A class which is updatable makes transparent the fact that it is built atop a listenable cache (in contrast to
 * a Serializable class). In return, it permits the developer a fine-grained control
 * of the methods it declares using the tag <i>Update</i>. When a method is tagged with this keyword,
 * the factory considers that it modifies the state of the object; otherwise the method is  perceived as read-only
 * allowing several performance optimizations.
 *
 * @author Pierre Sutra
 * @since 7.0
 */

public abstract class Updatable implements Externalizable{
   private transient Cache cache = null;
   private transient Object key = null;

   public Object getKey() {
      return key;
   }

   public Cache getCache() {
      return cache;
   }

   public void setKey(Object key) {
      this.key = key;
   }

   public void setCache(Cache cache) {
      this.cache = cache;
   }

}

