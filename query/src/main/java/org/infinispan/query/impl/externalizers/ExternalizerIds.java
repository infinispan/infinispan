package org.infinispan.query.impl.externalizers;

/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers.
 * For details, read http://infinispan.org/docs/9.0.x/user_guide/user_guide.html#_preassigned_externalizer_id_ranges
 *
 * The range reserved for the Infinispan Query module is from 1600 to 1699.
 *
 * @author Sanne Grinovero
 * @since 7.0
 */
public interface ExternalizerIds {

   Integer ICKLE_FILTER_AND_CONVERTER = 1600;

   Integer LUCENE_QUERY_BOOLEAN = 1601;

   Integer LUCENE_QUERY_TERM = 1602;

   Integer LUCENE_TERM = 1603;

   Integer LUCENE_SORT = 1604;

   Integer LUCENE_SORT_FIELD = 1605;

   Integer LUCENE_TOPDOCS = 1606;

   Integer CLUSTERED_QUERY_TOPDOCS = 1607;

   Integer LUCENE_SCORE_DOC = 1608;

   Integer LUCENE_TOPFIELDDOCS = 1609;

   Integer LUCENE_FIELD_SCORE_DOC = 1610;

   Integer ICKLE_FILTER_RESULT = 1611;

   Integer LUCENE_QUERY_MATCH_ALL = 1612;

   Integer INDEX_WORKER = 1613;

   Integer ICKLE_CACHE_EVENT_FILTER_CONVERTER = 1614;

   Integer LUCENE_BYTES_REF = 1615;

   Integer ICKLE_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER = 1616;

   Integer ICKLE_CONTINUOUS_QUERY_RESULT = 1617;
}
