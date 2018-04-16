package me.escoffier.infinispan.feed;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.CompletableHelper;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static me.escoffier.infinispan.feed.Utils.*;

public class FeedVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LogManager.getLogger("Feed-Verticle");

    //TODO it requires to be deployed in the same project as openwhisk
    private static final String OPENWHISK_ROOT = "http://" + System.getenv("NGINX_SERVICE_HOST");

    private static final String OPENWHISK_TRIGGER_API_PATH = "/api/v1/namespaces/_/triggers/";

    private WebClient client;

    private Map<String, Supplier<Completable>> cleaners = new HashMap<>();

    @Override
    public void start(Future<Void> fut) {
        Router router = Router.router(vertx);
        client = WebClient.create(vertx, new WebClientOptions().setSsl(true).setTrustAll(true));
        router.post().handler(BodyHandler.create());
        router.post("/api/feed/listener").handler(this::registerListener);
        router.delete("/api/feed/listener/:encoded").handler(this::unregisterListener);
        router.get("/health").handler(rc -> rc.response().end("OK"));
        router.route().handler(rc -> {
            LOGGER.info("Invoked on {} {} with body {}", rc.request().method(), rc.request().path(), rc.getBody());
            rc.response().end("{}");
        });

        vertx.createHttpServer()
            .requestHandler(router::accept)
            .rxListen(8080)
            .toCompletable()
            .subscribe(CompletableHelper.toObserver(fut));

    }

    private void unregisterListener(RoutingContext rc) {
        String triggerName = decode(rc.pathParam("encoded"));
        Supplier<Completable> supplier = cleaners.remove(triggerName);
        if (supplier == null) {
            rc.response().end(new JsonObject().put("message", "trigger not found: " + triggerName + " / " + cleaners.keySet())
                .encode());
        } else {
            LOGGER.info("Un-registering trigger {}" + triggerName);
            supplier.get().subscribe(
                () -> rc.response().end(new JsonObject().put("message", "trigger " + triggerName + " removed").encode()),
                rc::fail
            );
        }
    }

    private void registerListener(RoutingContext rc) {
        JsonObject json = rc.getBodyAsJson();
        LOGGER.info("Register listener called with " + json.encode());
        String cache = json.getString("cache_name", "default");

        String triggerName = rc.getBodyAsJson().getString("triggerName");
        if (cleaners.containsKey(triggerName)) {
            rc.response()
                .setStatusCode(400)
                .end(new JsonObject().put("message", "Trigger " + triggerName + " already registered").encode());
            return;
        }

        getRemoteCacheManager(json.getString("hotrod_server_host"), json.getInteger("hotrod_port", 11222))
            .flatMap(rcm -> retrieveCache(rcm, cache))
            .doOnSuccess(c -> LOGGER.info("Cache has been retrieved: {}", c.getName()))
            .flatMapCompletable(c -> registerListener(c, json))
            .subscribe(
                () -> rc.response().end(new JsonObject().put("message", "listener registered for trigger: " + triggerName).encode()),
                rc::fail
            );
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

    private Completable registerListener(RemoteCache cache, JsonObject json) {
        String auth = json.getString("authKey");
        String triggerName = json.getString("triggerName");

        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(cache.getName());
        consumer.handler(message -> {
            String key = message.body().getString("key");
            String value = message.body().getString("value");
            String event = message.body().getString("event");
            JsonObject body = new JsonObject();
            body.put("timestamp", System.currentTimeMillis())
                .put("eventType", event)
                .put("value", value)
                .put("key", key);

            String uri = OPENWHISK_ROOT + OPENWHISK_TRIGGER_API_PATH + extractTriggerName(triggerName);

            LOGGER.info("Posting to {} : {}", uri, body.encode());
            client.postAbs(uri)
                .putHeader("Authorization", "Basic " + encode(auth))
                .putHeader("Accept", "application/json")
                .rxSendJsonObject(body)
                .doOnSuccess(resp ->
                    LOGGER.info("Got result from trigger: {} {}", resp.statusCode(), resp.bodyAsString()))
                .subscribe();
        });

        return consumer
            .rxCompletionHandler()
            .andThen(
                vertx.rxExecuteBlocking(future -> {
                    RemoteCacheListener listener = new RemoteCacheListener(vertx, cache);

                    Supplier<Completable> cleaner = () ->
                        consumer.rxUnregister()
                            .andThen(
                                vertx.rxExecuteBlocking(
                                    fut -> {
                                        cache.removeClientListener(listener);
                                        cleaners.remove(triggerName);
                                        fut.complete();
                                    })
                                    .toCompletable()
                                    .onErrorResumeNext(t -> Completable.complete())
                            );

                    cleaners.put(triggerName, cleaner);
                    cache.addClientListener(listener);
                    LOGGER.info("Registering listener for trigger {}, ({})", triggerName, cleaners.keySet());
                    future.complete();
                })
                    .toCompletable());
    }


}
