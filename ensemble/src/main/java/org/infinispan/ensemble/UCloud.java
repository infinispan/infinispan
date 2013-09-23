package org.infinispan.ensemble;

import net.killa.kept.KeptConcurrentMap;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author Pierre Sutra
 * @since 4.0
 */

@XmlRootElement
public class UCloud {

    private static ConcurrentMap<String, UCloud> uclouds;
    static{
        uclouds = ZkManager.getInstance().newConcurrentMap("uclouds");
    }

    @XmlAttribute
    private String name;

    public UCloud(String name){
        this.name = name;
        uclouds.putIfAbsent(name, this);
    }

    public String getName(){
        return name;
    }

    @Override
    public boolean equals(Object o){
        if(!(o instanceof UCloud)) return false;
        return ((UCloud)o).getName().equals(this.getName());
    }

}
