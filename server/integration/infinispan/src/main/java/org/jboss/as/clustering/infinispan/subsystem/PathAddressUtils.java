package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;

/**
 * PathAddressUtils.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class PathAddressUtils {

    public static int indexOf(PathAddress parentAddress, PathElement element) {
        int size = parentAddress.size();
        String key = element.getKey();
        for(int i = 0; i < size; i++) {
            PathElement p = parentAddress.getElement(i);
            if (p.getKey().equals(key))
                return i;
        }
        return -1;
    }

}
