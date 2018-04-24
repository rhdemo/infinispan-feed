package me.escoffier.infinispan.feed.persistence;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.infinispan.feed.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class FileBasedPersistenceServiceTest {


    private Vertx vertx;
    private static final String ROOT = "target/test-classes/data";
    private PersistenceService service;
    private File file = new File(ROOT, Utils.encode("foo"));
    ;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
        service = PersistenceService.file(vertx, ROOT);
    }

    @After
    public void tearDown() {
        vertx.close();
        if (file != null  && file.isFile()) {
            file.delete();
        }
    }

    @Test
    public void testSave() {
        JsonObject document = new JsonObject().put("foo", "bar");
        Completable completable = service.save("foo", document);
        completable.blockingAwait();

        assertThat(file).isFile().hasContent(document.encode());

        JsonObject object = service.get("foo").blockingGet();
        assertThat(object).isEqualTo(document.put("_id", "foo"));
    }

    @Test
    public void testAll() {
        List<JsonObject> list = service.all().toList().blockingGet();
        assertThat(list).hasSize(1).containsExactly(new JsonObject().put("hello", "world").put("_id", "0"));

        JsonObject document = new JsonObject().put("foo", "bar");
        Completable completable = service.save("foo", document);
        completable.blockingAwait();

        list = service.all().toList().blockingGet();
        assertThat(list).hasSize(2)
            .contains(new JsonObject().put("hello", "world").put("_id", "0"))
            .contains(new JsonObject().put("foo", "bar").put("_id", "foo"));

        JsonObject object = service.get("foo").blockingGet();
        assertThat(object).isEqualTo(document.put("_id", "foo"));

        assertThat(service.get("0").blockingGet()).isEqualTo(new JsonObject().put("hello", "world").put("_id", "0"));
    }

    @Test
    public void testGet() {
        assertThat(service.get("0").blockingGet()).isEqualTo(new JsonObject().put("hello", "world").put("_id", "0"));
        assertThat(service.get("missing").blockingGet()).isNull();
    }

    @Test
    public void testDelete() {
        JsonObject document = new JsonObject().put("foo", "bar");
        Completable completable = service.save("foo", document);
        completable.blockingAwait();

        assertThat(service.all().toList().blockingGet()).hasSize(2);

        service.delete("foo").blockingAwait();

        assertThat(service.all().toList().blockingGet()).hasSize(1);

        try {
            service.delete("missing").blockingAwait();
            fail("Missing file not detected");
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("NoSuchFileException");
        }

        assertThat(service.all().toList().blockingGet()).hasSize(1);
    }

}