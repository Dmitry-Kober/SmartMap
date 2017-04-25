package org.smartsoftware.smartmap.domain.data;

/**
 * Created by Dmitry on 24.04.2017.
 */
public class StringKey implements IKey<String> {

    private final String key;

    public StringKey(String key) {
        this.key = key;
    }

    @Override
    public String get() {
        return key;
    }
}
