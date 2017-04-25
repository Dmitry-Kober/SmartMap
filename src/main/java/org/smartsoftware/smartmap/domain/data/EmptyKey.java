package org.smartsoftware.smartmap.domain.data;

/**
 * Created by dkober on 25.4.2017 г..
 */
public class EmptyKey implements IKey<String> {

    @Override
    public String get() {
        return "";
    }
}
