package me.escoffier.infinispan.feed.persistence;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import me.escoffier.infinispan.feed.Utils;

import java.io.File;

/**
 * Implementation of the {@link PersistenceService} based on the file system.
 * <p>
 * In the given root directory, each document is stored in a file named "id" (id being the id of the document). When
 * retrieve the "_id"_ field is injected into the document.
 */
public class FileBasedPersistenceService implements PersistenceService {
    private final Vertx vertx;
    private final String root;

    public FileBasedPersistenceService(Vertx vertx, String root) {
        this.vertx = vertx;
        this.root = root;
    }

    private String encode(String id) {
        return Utils.encode(id);
    }

    @Override
    public Completable save(String id, JsonObject json) {
        return vertx.fileSystem().rxWriteFile(root + "/" + encode(id), new Buffer(json.toBuffer()));
    }

    @Override
    public Maybe<JsonObject> get(String id) {
        return vertx.fileSystem().rxReadFile(root + "/" + encode(id))
            .map(buffer -> buffer.toJsonObject().put("_id", id))
            .flatMapMaybe(Maybe::just)
            .onErrorComplete();
    }

    @Override
    public Flowable<JsonObject> all() {
        return vertx.fileSystem().rxReadDir(root)
            .flatMapPublisher(Flowable::fromIterable)
            .filter(s -> ! s.contains("+"))
            .flatMapSingle(path -> vertx.fileSystem().rxReadFile(path)
                .map(buffer -> buffer.toJsonObject().put("_id", Utils.decode(path.substring(path.lastIndexOf("/") + 1)))));
    }

    @Override
    public Completable delete(String id) {
        return vertx.fileSystem().rxDelete(root + "/" + encode(id));
    }
}
