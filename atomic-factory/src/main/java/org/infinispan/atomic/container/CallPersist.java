package org.infinispan.atomic.container;

/**
* @author Pierre Sutra
* @since 7.2
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
