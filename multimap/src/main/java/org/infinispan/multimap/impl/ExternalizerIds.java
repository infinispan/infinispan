package org.infinispan.multimap.impl;

/**
 * Ids range: 2050 - 2099
 * Externalizer Ids that identity the functions used in {@link EmbeddedMultimapCache}
 *
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @author Katia Aresti - karesti@redhat.com
 * @since 9.2
 */
public interface ExternalizerIds {
   Integer PUT_KEY_VALUE_FUNCTION = 2050;
   Integer REMOVE_KEY_VALUE_FUNCTION = 2051;
   Integer CONTAINS_KEY_VALUE_FUNCTION = 2052;
   Integer GET_FUNCTION = 2053;
}
