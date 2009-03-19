package org.horizon.container;

/**
 * // TODO: Manik: Document this!
 *
 * @author Manik Surtani
 */
public interface UpdateableEntry extends MVCCEntry {
   void copyForUpdate(DataContainer container, boolean writeSkewCheck);
}
