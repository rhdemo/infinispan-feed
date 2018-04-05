package me.escoffier.infinispan.feed;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class UtilsTest {

    @Test
    public void testShortNameExtraction() {
        String name = Utils.extractTriggerName("/_/object-written");
        assertThat(name, is("object-written"));
    }

    @Test
    public void base64Test() {
        String s = Utils.encode("foofoo");
        assertThat(s, is(notNullValue()));
    }

}