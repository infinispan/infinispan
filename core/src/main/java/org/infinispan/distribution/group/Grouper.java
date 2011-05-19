package org.infinispan.distribution.group;

/**
 * <p>
 * User applications may implement this interface in order to customize the compution of groups in cases when the modifying the
 * key is not possible, or when the value determined by the {@link Group} annotation needs customizing.
 * </p>
 * 
 * <p>
 * <code>Grouper</code> acts as an interceptor, passing the previously computed value in. The group passed to the first
 * <code>Grouper</code> will be that determined by <code>@Group</code> (if <code>@Group</code> is defined).
 * </p>
 * 
 * <p>
 * For example:
 * </p>
 * 
 * <pre>
 * public class KXGrouper implements Grouper&lt;String&gt; {
 * 
 *     // A pattern that can extract from a &quot;kX&quot; (e.g. k1, k2) style key
 *     private static Pattern kPattern = Pattern.compile(&quot;(&circ;k)(\\d)$&quot;);
 * 
 *     public String computeGroup(String key, String group) {
 *         Matcher matcher = kPattern.matcher(key);
 *         if (matcher.matches()) {
 *             String g = Integer.parseInt(matcher.group(2)) % 2 + &quot;&quot;;
 *             return g;
 *         } else
 *             return null;
 *     }
 * 
 *     public Class&lt;String&gt; getKeyType() {
 *         return String.class;
 *     }
 * 
 * }
 * </pre>
 * 
 * <p>
 * You must set the
 * <code>groupsEnabled<code> property to true in your configuration in order to use groups. You can specify an order list of groupers there.
 * </p>
 * 
 * @see Group
 * 
 * @author Pete Muir
 * 
 * @param <T>
 */
public interface Grouper<T> {

    /**
     * Compute the group for a given key
     * 
     * @param key the key to compute the group for
     * @param group the group as currently computed, or null if no group has been determined yet
     * @return the group, or null if no group is defined
     */
    String computeGroup(T key, String group);

    Class<T> getKeyType();

}