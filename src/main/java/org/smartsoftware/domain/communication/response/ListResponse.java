package org.smartsoftware.domain.communication.response;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by dkober on 25.4.2017 г..
 */
public class ListResponse implements IResponse {

    private final List<String> list = new LinkedList<>();

    public ListResponse(List<String> list) {
        this.list.addAll(list);
    }

    public List<String> getList() {
        return new LinkedList<>(list);
    }
}
