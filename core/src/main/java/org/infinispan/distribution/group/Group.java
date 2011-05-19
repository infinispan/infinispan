package org.infinispan.distribution.group;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * <p>
 * Identifies the key for a group.
 * </p>
 * 
 * <p>
 * <code>@Group</code> should be used when you have control over the key class. For example:
 * </p>
 * 
 * <pre>
 * class User {
 * 
 *    ...
 *    String office;
 *    ...
 *    
 *    int hashCode() {
 *       // Defines the hash for the key, normally used to determine location
 *       ...
 *    }
 *    
 *    // Override the location by specifying a group, all keys in the same 
 *    // group end up with the same owner
 *    @Group
 *    String getOffice() {
 *       return office;
 *    }
 *    
 * }
 * </pre>
 * 
 * <p>
 * If you don't have control over the key class, you can specify a {@link Grouper} (in your configuration) which can be used to
 * specify the group externally.
 * </p>
 * 
 * <p>
 * You must set the <code>groupsEnabled<code> property to true in your configuration in order to use groups.
 * </p>
 * 
 * @see Grouper
 * 
 * @author Pete Muir
 * 
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface Group {

}
