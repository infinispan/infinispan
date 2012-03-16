package org.infinispan.distribution.groups;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.distribution.group.Grouper;

/**
 * A simple grouper which groups String based keys using a pattern for kX keys
 * @author Pete Muir
 *
 */
public class KXGrouper implements Grouper<String> {
    
    private static Pattern kPattern = Pattern.compile("(^k)(\\d)$"); 

    @Override
    public String computeGroup(String key, String group) {
        Matcher matcher = kPattern.matcher(key);
        if (matcher.matches()) {
            String g = Integer.parseInt(matcher.group(2)) % 2 + "";
            return g;
        }
        else
            return null;
    }
    
    @Override
    public Class<String> getKeyType() {
        return String.class;
    }
    
    
}
