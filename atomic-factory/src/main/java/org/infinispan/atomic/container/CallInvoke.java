package org.infinispan.atomic.container;

/**
* *
* @author Pierre Sutra
* @since 6.0
*/
class CallInvoke extends Call {
    String method;
    Object[] arguments;

    public CallInvoke(long id, String m, Object[] args) {
        super(id);
        method = m;
        arguments = args;
    }

    @Override
    public String toString(){
        String args = " ";
        for(Object a : arguments){
            args+=a.toString()+" ";
        }
        return super.toString()+"  - INV - "+method+ " ("+args+")";
    }
}
