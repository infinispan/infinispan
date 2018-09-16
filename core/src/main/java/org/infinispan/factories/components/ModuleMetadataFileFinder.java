package org.infinispan.factories.components;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;

/**
 * This interface should be implemented by all Infinispan modules that expect to have components using {@link Inject},
 * {@link Start} or {@link Stop} annotations.  The metadata file is generated at build time and packaged in the module's
 * corresponding jar file (see Infinispan's <pre>core</pre> module <pre>pom.xml</pre> for an example of this).
 * <p>
 * Module component metadata is usually generated in a file titled <pre>${module-name}-component-metadata.dat</pre> and
 * typically resides in the root of the module's jar file.
 * <p>
 * For example, Infinispan's Query Module would implement this interface to return <pre>infinispan-query-component-metadata.dat</pre>.
 * <p>
 * Implementations of this interface are discovered using the JDK's {@link java.util.ServiceLoader} utility.  Which means
 * modules would also have to package a file called <pre>org.infinispan.factories.components.ModuleMetadataFileFinder</pre>
 * in the <pre>META-INF/services/</pre> folder in their jar, and this file would contain the fully qualified class name
 * of the module's implementation of this interface.
 * <p>
 * Please see Infinispan's query module for an example of this.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public interface ModuleMetadataFileFinder {
   String getMetadataFilename();
}
