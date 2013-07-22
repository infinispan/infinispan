package org.infinispan.atomic;

/**
* // TODO: Document this
*
* @author otrack
* @since 4.0
*/
class AtomicObjectCallPersist extends AtomicObjectCall{
    Object object;
    public AtomicObjectCallPersist(int id, Object o) {
        super(id);
        object = o;
    }
}
