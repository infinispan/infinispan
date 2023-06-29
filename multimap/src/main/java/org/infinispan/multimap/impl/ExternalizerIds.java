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
   Integer SET_ADD_FUNCTION = 2064;
   Integer TRIM_FUNCTION = 2065;
   Integer ROTATE_FUNCTION = 2066;
   Integer SORTED_SET_ADD_MANY_FUNCTION = 2067;
   Integer HASH_MAP_KEYSET_FUNCTION = 2068;
   Integer HASH_MAP_VALUES_FUNCTION = 2069;
   Integer SORTED_SET_COUNT_FUNCTION = 2070;
   Integer SORTED_SET_POP_FUNCTION = 2071;
}
