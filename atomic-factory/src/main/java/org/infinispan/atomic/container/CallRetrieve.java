package org.infinispan.atomic.container;

/**
*
* @author Pierre Sutra
* @since 6.0
*/
class CallRetrieve extends Call {
    public CallRetrieve(long id) {
        super(id);
    }

    @Override
    public String toString() {
        return super.toString()+"(RET)";
    }


}
