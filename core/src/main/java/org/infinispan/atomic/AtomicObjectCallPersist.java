package org.infinispan.atomic;

/**
*
* @author Pierre Sutra
* @since 6.0
*/
class AtomicObjectCallPersist extends AtomicObjectCall{
    Object object;
    public AtomicObjectCallPersist(int id, Object o) {
        super(id);
        object = o;
    }
}
