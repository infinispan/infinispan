/**
 * It must be in a diff package because we add @{link AutodiscoverRemoteExtension} into the war.
 * Oherwise, it will be adding a lot of other packages that are not required like: net.bytebuddy
 */
package org.infinispan.server.integration.enricher;
