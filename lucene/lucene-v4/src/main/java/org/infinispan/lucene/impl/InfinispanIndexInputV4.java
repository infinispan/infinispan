package org.infinispan.lucene.impl;


/**
 * Responsible for reading from <code>InfinispanDirectory</code>.
 * This compile unit is separate than InfinispanIndexInput as it needs
 * to be compiled in the context of Lucene 4.
 *
 * @since 5.2
 * @author Sanne Grinovero
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.IndexInput
 */
public final class InfinispanIndexInputV4 extends InfinispanIndexInput {

   public InfinispanIndexInputV4(IndexInputContext openInput) {
      super(openInput);
   }

   @Override
   public InfinispanIndexInputV4 clone() {
      InfinispanIndexInputV4 clone = (InfinispanIndexInputV4)super.clone();
      // reference counting doesn't work properly: need to use isClone
      // as in other Directory implementations. Apparently not all clones
      // are cleaned up, but the original is (especially .tis files)
      clone.isClone = true;
      return clone;
    }

}
