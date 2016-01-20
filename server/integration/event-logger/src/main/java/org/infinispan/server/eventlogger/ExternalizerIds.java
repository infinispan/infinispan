package org.infinispan.server.eventlogger;

/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers.
 * For details, read http://infinispan.org/docs/8.2.x/user_guide/user_guide.html#_preassigned_externalizer_id_ranges
 *
 * The range reserved for the Infinispan Server Event Logger module is from 1850 to 1899
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public interface ExternalizerIds {

   Integer SERVER_EVENT = 1850;

}
