package org.infinispan.atomic.container;

/**
*
* @author Pierre Sutra
* @since 6.0
*/
class CallPersist extends Call {
    Object object;
    public CallPersist(long id, Object o) {
        super(id);
        object = o;
    }

    @Override
    public String toString() {
        return super.toString()+"(PER)";
    }
}
