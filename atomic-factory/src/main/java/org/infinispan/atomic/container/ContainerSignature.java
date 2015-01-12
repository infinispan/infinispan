package org.infinispan.atomic.container;

/**
*
* @author Pierre Sutra
* @since 7.0
 *
*/
public class ContainerSignature {

    private Class clazz;
    private Object key;

    public ContainerSignature(Class c, Object k){
        clazz = c;
        key = k;
    }

    @Override
    public int hashCode(){
        return clazz.hashCode() + key.hashCode();
    }

    @Override
    public boolean equals(Object o){
        if (!(o instanceof ContainerSignature))
            return false;
        return ((ContainerSignature)o).clazz.equals(this.clazz)
                && ((ContainerSignature)o).key.equals(this.key);
    }

    @Override
    public String toString(){
        return key.toString()+"["+clazz.getSimpleName()+"]";
    }

}
