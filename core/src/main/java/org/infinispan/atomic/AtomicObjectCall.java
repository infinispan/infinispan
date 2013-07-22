package org.infinispan.atomic;

import java.io.Serializable;

/**
* // TODO: Document this
*
* @author otrack
* @since 4.0
*/
abstract class AtomicObjectCall implements Serializable {
    int callID;
    public AtomicObjectCall(int id){
        callID = id;
    }
}
