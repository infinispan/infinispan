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
   Integer OFFER_FUNCTION = 2054;
   Integer INDEX_FUNCTION = 2055;
   Integer POLL_FUNCTION = 2056;
   Integer SET_FUNCTION = 2057;
   Integer SUBLIST_FUNCTION = 2058;
   Integer INDEXOF_FUNCTION = 2059;
   Integer INSERT_FUNCTION = 2060;
   Integer REMOVE_COUNT_FUNCTION = 2061;
   Integer HASH_MAP_PUT_FUNCTION = 2062;
   Integer MULTIMAP_CONVERTER = 2063;
   Integer ADD_FUNCTION = 2064;
}
