package org.infinispan.util.concurrent.locks;

import java.util.concurrent.locks.ReentrantLock;

/**
* Extends {@link ReentrantLock} only to make the {@link #getOwner()} method public.
*
* @author Dan Berindei
* @since 5.2
*/
public class VisibleOwnerReentrantLock extends ReentrantLock {
   @Override
   public Thread getOwner() {
      return super.getOwner();
   }
}
