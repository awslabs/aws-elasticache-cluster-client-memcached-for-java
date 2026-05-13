package net.spy.memcached.config;

/**
 * Factory for {@link ConfigEndpointSelectionStrategy} instances.
 *
 * <p>A {@code ConfigEndpointSelectionStrategy} carries mutable per-client
 * session state (for example a cached configuration {@link
 * net.spy.memcached.MemcachedConnection} and a round-robin cursor). Sharing a
 * single strategy instance across multiple
 * {@link net.spy.memcached.MemcachedClient}s would therefore leak that state
 * between clients.</p>
 *
 * <p>To avoid that, callers configure a {@code ConfigEndpointSelectionStrategyFactory}
 * rather than a strategy instance. The connection factory invokes
 * {@link #create()} once per client, guaranteeing each client gets its own
 * isolated strategy without any need for cloning.</p>
 */
public interface ConfigEndpointSelectionStrategyFactory {
    /**
     * Create a brand new strategy instance with independent session state.
     *
     * @return a fresh {@link ConfigEndpointSelectionStrategy}
     */
    ConfigEndpointSelectionStrategy create();
}
