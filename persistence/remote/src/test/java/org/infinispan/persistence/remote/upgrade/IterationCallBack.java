package org.infinispan.persistence.remote.upgrade;

interface IterationCallBack {
   void iterationReached(Object key);
}
