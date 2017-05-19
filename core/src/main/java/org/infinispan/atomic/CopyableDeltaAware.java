package org.infinispan.atomic;

/**
 * This interface extended the {@link org.infinispan.atomic.DeltaAware}. The copy allows to use Copy-On-Write semantic
 * needed to ensure the correct transaction isolation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 * @deprecated since 9.1
 */
@Deprecated
public interface CopyableDeltaAware extends DeltaAware {

   /**
    * @return a copy of this DeltaAware instance.
    */
   CopyableDeltaAware copy();

}
