package org.infinispan.query.impl.externalizers;


/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers.
 * For details, read http://infinispan.org/docs/7.0.x/user_guide/user_guide.html#_preassigned_externalizer_id_ranges
 *
 * The range reserved for the Infinispan Query module is from 1600 to 1699.
 *
 * @author Sanne Grinovero
 * @since 7.0
 */
public class ExternalizerIds {

   public static final Integer CLUSTERED_QUERY_TOPDOCS = 1600;

   public static final Integer LUCENE_QUERY_BOOLEAN = 1650;

   public static final Integer LUCENE_QUERY_TERM = 1651;

   public static final Integer LUCENE_TERM = 1640;

   public static final Integer LUCENE_SORT = 1641;

   public static final Integer LUCENE_SORT_FIELD = 1642;

   public static final Integer LUCENE_TOPDOCS = 1643;

   public static final Integer LUCENE_SCORE_DOC = 1644;

   public static final Integer LUCENE_TOPFIELDDOCS = 1645;


}
