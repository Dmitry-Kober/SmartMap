package org.smartsoftware.request.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.request.domain.IRequest;
import org.smartsoftware.request.manager.datasource.IShardDAO;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * Created by Dmitry on 23.04.2017.
 */
@Component
public class HashBasedRequestManager implements IRequestManager {

    private static final Logger LOG = LoggerFactory.getLogger(HashBasedRequestManager.class);
    private final BlockingQueue<IRequest> requests = new ArrayBlockingQueue<IRequest>(1000);
    private final Map<String, Shard> shards;



    HashBasedRequestManager(Map<String, Shard> shards) {
        this.shards = shards;
    }

    @PostConstruct
    public void init() {
        LOG.trace("Initializing a Request Manager...");

        // initialize shards
        shards.values().stream().forEach(shard -> {
            shard.getFileSystem().init();
            shard.getDao().init();
        });
    }


}
