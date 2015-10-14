package org.infinispan.cli.interpreter.codec;

import org.infinispan.Cache;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.rpc.RpcManager;

/**
 * AbstractCodec.
 *
 * @author Tristan Tarrant
 * @since 6.1
 */
public abstract class AbstractCodec implements Codec {
    @Override
    public Metadata encodeMetadata(Cache<?, ?> cache, Long expires, Long maxIdle) {
        EmbeddedMetadata.Builder metadata = new EmbeddedMetadata.Builder();
        metadata.version(generateVersion(cache));
        if (expires != null) {
            metadata.lifespan(expires);
        }
        if (maxIdle != null) {
            metadata.maxIdle(maxIdle);
        }

        return metadata.build();
    }

    protected EntryVersion generateVersion(Cache<?, ?> cache) {
        ComponentRegistry registry = cache.getAdvancedCache().getComponentRegistry();
        VersionGenerator versionGenerator = registry.getVersionGenerator();
        if (versionGenerator == null) {
            // This is now superfluous, as the component registry always creates the VersionGenerator on startup
           VersionGenerator newVersionGenerator = new NumericVersionGenerator().clustered(registry.getComponent(RpcManager.class) != null);
           registry.registerComponent(newVersionGenerator, VersionGenerator.class);
           registry.cacheComponents();
           return newVersionGenerator.generateNew();
        } else {
           return versionGenerator.generateNew();
        }
     }
}
