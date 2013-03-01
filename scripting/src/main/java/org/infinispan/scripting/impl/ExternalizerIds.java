package org.infinispan.scripting.impl;

/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers.
 * For details, read http://infinispan.org/docs/7.0.x/user_guide/user_guide.html#_preassigned_externalizer_id_ranges
 *
 * The range reserved for the Infinispan Scripting module is from 1800 to 1899.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface ExternalizerIds {

   Integer SCRIPT_METADATA = 1800;

}
