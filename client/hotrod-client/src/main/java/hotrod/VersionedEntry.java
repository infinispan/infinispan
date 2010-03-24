package hotrod;

import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface VersionedEntry extends Map.Entry{
   public long getVersion();
}
