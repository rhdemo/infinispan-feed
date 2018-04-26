package me.escoffier.infinispan.feed;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.CompletableHelper;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import me.escoffier.infinispan.feed.persistence.PersistenceService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static me.escoffier.infinispan.feed.Utils.*;

public class FeedVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LogManager.getLogger("Feed-Verticle");

    //TODO it requires to be deployed in the same project as openwhisk
    private static final String OPENWHISK_ROOT = "http://" + System.getenv("NGINX_SERVICE_HOST");

    private static final String OPENWHISK_TRIGGER_API_PATH = "/api/v1/namespaces/_/triggers/";

    private WebClient client;

    private PersistenceService service;
    private Map<String, Supplier<Completable>> cleaners = new HashMap<>();
    private Map<String, Supplier<Completable>> checks = new HashMap<>();

    @Override
    public void start(Future<Void> fut) {
        service = PersistenceService.file(vertx, "/data");
        Router router = Router.router(vertx);
        client = WebClient.create(vertx, new WebClientOptions().setSsl(true).setTrustAll(true));
        router.post().handler(BodyHandler.create());
        router.post("/api/feed/listener").handler(this::registerListener);
        router.delete("/api/feed/listener/:encoded").handler(this::unregisterListener);

        router.get("/listeners").handler(this::list);

        router.get("/health").handler(rc -> {
            if (checks.isEmpty()) {
                rc.response().end("OK - no listener registered");
            } else {
                AtomicBoolean failureDetected = new AtomicBoolean();
                AtomicInteger count = new AtomicInteger();
                StringBuffer sb = new StringBuffer();
                checks.forEach((name, check) ->
                    check.get()
                        .doFinally(() -> {
                            int c = count.incrementAndGet();
                            if (c == checks.size()) {
                                if (failureDetected.get()) {
                                    rc.response().setStatusCode(503).end(sb.toString());
                                } else {
                                    rc.response().setStatusCode(200).end(sb.toString());
                                }
                            }
                        })
                        .subscribe(
                            () -> sb.append(name).append(" OK \n"),
                            err -> {
                                sb.append(name).append(" KO - ").append(err.getMessage()).append("\n");
                                failureDetected.set(true);
                            }
                        ));
            }
        });
        router.route().handler(rc -> {
            LOGGER.info("Invoked on {} {} with body {}", rc.request().method(), rc.request().path(), rc.getBody());
            rc.response().end("{}");
        });


        Completable reload = service.all()
            .flatMapCompletable(json -> {
                LOGGER.info("Reloading trigger from " + json.encode());
                return register(json);
            })
            .doOnComplete(() -> LOGGER.info("{} listeners reloaded", cleaners.size()));

        reload
            .andThen(vertx.createHttpServer()
                .requestHandler(router::accept)
                .rxListen(8080)
                .toCompletable()
            )
            .subscribe(CompletableHelper.toObserver(fut));

    }

    private void list(RoutingContext rc) {
        service.all().toList().map(list -> list.stream().collect(JsonArray::new, JsonArray::add, JsonArray::addAll))
            .subscribe(
                array -> rc.response().end(array.encodePrettily()),
                rc::fail);
    }

    private void unregisterListener(RoutingContext rc) {
        String triggerName = decode(rc.pathParam("encoded"));
        service.get(triggerName)
            .switchIfEmpty(Single.error(new Exception("trigger not found " + triggerName)))
            .flatMapCompletable(json -> {
                LOGGER.info("Un-registering trigger {}" + triggerName);
                return cleanup(triggerName);
            })
            .subscribe(
                () -> rc.response().end(new JsonObject().put("message", "trigger " + triggerName + " removed").encode()),
                err -> rc.response().setStatusCode(400).end(new JsonObject().put("message", err.getMessage()).encode())
            );
    }

    private void registerListener(RoutingContext rc) {
        JsonObject json = rc.getBodyAsJson();
        LOGGER.info("Register listener called with " + json.encode());

        String triggerName = rc.getBodyAsJson().getString("triggerName");

        service.get(triggerName)
            .doOnSuccess(x -> {
                throw new Exception("Trigger " + triggerName + " already registered");
            })
            .switchIfEmpty(register(json).toSingleDefault(json))
            .subscribe(
                j -> rc.response()
                    .end(new JsonObject().put("message", "listener registered for trigger: " + triggerName).encode()),
                err -> rc.response()
                    .setStatusCode(400)
                    .end(new JsonObject().put("message", err.getMessage()).encode())
            );
    }

    private Completable cleanup(String triggerName) {
        Supplier<Completable> supplier = cleaners.remove(triggerName);
        checks.remove(triggerName);
        if (supplier == null) {
            return Completable.error(new RuntimeException("Cannot find the cleaner for " + triggerName));
        }
        return service.delete(triggerName)
            .doOnComplete(() -> LOGGER.info("Document deleted: {}", triggerName))
            .andThen(supplier.get());
    }


    private Completable register(JsonObject json) {
        String host = json.getString("hotrod_server_host");
        Integer port = json.getInteger("hotrod_port", 11222);
        String cache = json.getString("cache_name", "default");
        return getRemoteCacheManager(host, port)
            .flatMap(rcm -> retrieveCache(rcm, cache))
            .doOnSuccess(c -> LOGGER.info("Cache has been retrieved: {}", c.getName()))
            .flatMapCompletable(rc -> setupListener(rc, json));
    }


    private Single<RemoteCache> retrieveCache(RemoteCacheManager rcm, String cache) {
        return vertx.rxExecuteBlocking(future -> future.complete(rcm.getCache(cache)));
    }

    private Single<RemoteCacheManager> getRemoteCacheManager(String host, Integer port) {
        return vertx.rxExecuteBlocking(
            future -> {
                ConfigurationBuilder cb = new ConfigurationBuilder();
                LOGGER.info("Infinispan location: {}:{}", host, port);
                cb.addServer()
                    .host(host)
                    .port(port);
                cb.maxRetries(999).connectionTimeout(60000);
                future.complete(new RemoteCacheManager(cb.build()));
            }
        );
    }

    private Completable setupListener(RemoteCache cache, JsonObject json) {
        String auth = json.getString("authKey");
        String triggerName = json.getString("triggerName");
        String uri = OPENWHISK_ROOT + OPENWHISK_TRIGGER_API_PATH + extractTriggerName(triggerName);

        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(cache.getName());
        consumer.toObservable()
            .map(Message::body)
            .distinctUntilChanged()
            .map(body -> {
                    String key = body.getString("key");
                    String value = body.getString("value");
                    String event = body.getString("event");
                    JsonObject payload = new JsonObject();
                    return payload.put("timestamp", System.currentTimeMillis())
                        .put("eventType", event)
                        .put("value", value)
                        .put("key", key);
                }
            )
            .flatMapSingle(payload -> {
                LOGGER.info("{} is posting to {} : {}", this, uri, payload.encode());
                return client.postAbs(uri)
                    .putHeader("Authorization", "Basic " + encode(auth))
                    .putHeader("Accept", "application/json")
                    .rxSendJsonObject(payload);
            })
            .doOnNext(resp ->
                LOGGER.info("{} got result from trigger: {} {}", this, resp.statusCode(), resp.bodyAsString()))
            .subscribe();

        return consumer
            .rxCompletionHandler()
            .andThen(vertx.rxExecuteBlocking(future -> {
                RemoteCacheListener listener = new RemoteCacheListener(vertx, cache);
                Supplier<Completable> cleaner = () ->
                    consumer.rxUnregister()
                        .andThen(
                            vertx.rxExecuteBlocking(
                                fut -> {
                                    cache.removeClientListener(listener);
                                    fut.complete();
                                })
                                .toCompletable()
                                .onErrorResumeNext(t -> Completable.complete())
                        );
                cleaners.put(triggerName, cleaner);
                checks.put(triggerName, () ->
                    vertx.rxExecuteBlocking(fut -> {
                        cache.size();
                        fut.complete();
                    }).toCompletable()
                );

                cache.addClientListener(listener);
                LOGGER.info("Registering client listener for trigger {}, ({})", triggerName, cleaners.keySet());

                future.complete();
            })
                .flatMapCompletable(x -> service.save(triggerName, json))
                .doOnComplete(() -> LOGGER.info("Document saved: {}", triggerName)));
    }


}
