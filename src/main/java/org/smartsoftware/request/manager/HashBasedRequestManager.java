package org.smartsoftware.request.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.request.domain.IRequest;
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
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Dmitry on 23.04.2017.
 */
@Component
public class HashBasedRequestManager implements IRequestManager {

    private static final Logger LOG = LoggerFactory.getLogger(HashBasedRequestManager.class);
    private final String dataLocationRoot;
    private final BlockingQueue<IRequest> requests = new ArrayBlockingQueue<IRequest>(1000);
    private final Map<String, DataSource> shards;

    HashBasedRequestManager(String dataLocationRoot, Map<String, DataSource> shards) {
        this.dataLocationRoot = dataLocationRoot;
        this.shards = shards;
    }

    @PostConstruct
    private void initialize() {
        LOG.trace("Initializing the Base Request Manager in: {}", dataLocationRoot);

        // identify a data location root
        Path dataLocationRootDir = Paths.get(dataLocationRoot);
        try {
            if ( ! Files.exists(dataLocationRootDir) ) {
                dataLocationRootDir = Files.createDirectories(dataLocationRootDir);
            }
        }
        catch (IOException e) {
            LOG.error("An error appeared during the BaseRequestManager initialization: ", e);
            throw new RuntimeException(e);
        }

        // initialize database shards
        for (Map.Entry<String, DataSource> shardEntry : shards.entrySet()) {
            try {
                // initialize folders
                Path shardPaths = Paths.get(shardEntry.getKey());
                if ( ! Files.exists(shardPaths) ) {
                    Files.createDirectories(shardPaths);
                }

                // initialize databases
                shardEntry.getValue().getConnection().isValid(1000);
            }
            catch (IOException e) {
                LOG.error("An error appeared during the BaseRequestManager initialization: ", e);
                throw new RuntimeException(e);
            } catch (SQLException e) {
                LOG.error("An error appeared during the BaseRequestManager initialization: ", e);
                throw new RuntimeException(e);
            }
        }
    }

    public void test() {

    }


}
