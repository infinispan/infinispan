package org.infinispan.distribution.groups;

import org.infinispan.distribution.group.Group;

public class GroupedKey {
    
    private final String group;
    private final String key;
    
    public GroupedKey(String group, String key) {
        this.group = group;
        this.key = key;
    }

    @Group
    public String getGroup() {
        return group;
    }
    
    @Override
    public int hashCode() {
        return key.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GroupedKey)
            return ((GroupedKey) obj).key.equals(this.key);
        else
            return false;
    }

}
