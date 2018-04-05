package me.escoffier.infinispan.feed;

import java.util.Base64;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Utils {

    public static String encode(String auth) {
        return Base64.getEncoder().encodeToString(auth.getBytes());
    }

    public static String decode(String auth) {
        return new String(Base64.getDecoder().decode(auth));
    }

    public static String extractTriggerName(String fullName) {
        int index = fullName.lastIndexOf("/");
        if (index != -1) {
            return fullName.substring(index + 1);
        }
        return fullName;
    }
}
