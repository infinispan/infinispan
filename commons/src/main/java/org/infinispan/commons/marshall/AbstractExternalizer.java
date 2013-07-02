package org.infinispan.commons.marshall;

/**
 * Base class for {@link AdvancedExternalizer} implementations that offers default
 * implementations for some of its methods. In particular, this base class
 * offers a default implementation for {@link org.infinispan.commons.marshall.AdvancedExternalizer#getId()}
 * that returns null which is particularly useful for advanced externalizers
 * whose id will be provided by XML or programmatic configuration rather than
 * the externalizer implementation itself.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public abstract class AbstractExternalizer<T> implements AdvancedExternalizer<T> {

   @Override
   public Integer getId() {
      return null;
   }

}
