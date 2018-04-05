package me.escoffier.infinispan.feed;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.commons.util.KeyValueWithPrevious;


@ClientListener(converterFactoryName = "key-value-with-previous-converter-factory")
public class RemoteCacheListener {
    private final Vertx vertx;
    private final String address;

    public RemoteCacheListener(Vertx vertx, RemoteCache cache) {
        this.vertx = vertx;
        this.address = cache.getName();
    }

    @ClientCacheEntryCreated
    public void handleCreatedEvent(ClientCacheEntryCustomEvent<KeyValueWithPrevious> e) {
        vertx.eventBus().send(address, new JsonObject().put("event", "CREATE")
            .put("value", e.getEventData().getValue())
            .put("key", e.getEventData().getKey()));
    }

    @ClientCacheEntryModified
    public void handleModifiedEvent(ClientCacheEntryCustomEvent<KeyValueWithPrevious> e) {
        vertx.eventBus().send(address, new JsonObject().put("event", "UPDATE")
            .put("value", e.getEventData().getValue())
            .put("key", e.getEventData().getKey()));
    }

    @ClientCacheEntryRemoved
    public void handleRemovedEvent(ClientCacheEntryCustomEvent<KeyValueWithPrevious> e) {
        vertx.eventBus().send(address, new JsonObject().put("event", "REMOVE")
            .put("value", e.getEventData().getValue())
            .put("key", e.getEventData().getKey()));
    }
}
