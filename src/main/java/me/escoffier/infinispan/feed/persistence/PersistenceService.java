package me.escoffier.infinispan.feed.persistence;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;

/**
 * Very simple persistence service
 */
public interface PersistenceService {

    static PersistenceService file(Vertx vertx, String root) {
        return new FileBasedPersistenceService(vertx, root);
    }

    Completable save(String id, JsonObject json);

    Maybe<JsonObject> get(String id);

    Flowable<JsonObject> all();

    Completable delete(String id);

}
