package org.smartsoftware.request.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.domain.communication.CommunicationChain;
import org.smartsoftware.domain.communication.request.GetRequest;
import org.smartsoftware.domain.communication.request.RemoveRequest;
import org.smartsoftware.domain.data.IKey;
import org.smartsoftware.domain.communication.request.IRequest;
import org.smartsoftware.domain.communication.request.PutRequest;
import org.smartsoftware.domain.data.IValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Created by Dmitry on 23.04.2017.
 */
@Component
public class HashBasedRequestManager implements IRequestManager {

    private static final Logger LOG = LoggerFactory.getLogger(HashBasedRequestManager.class);

    @Value("file.manipulation.attempt.limit")
    private int FILE_MANIPULATION_ATTEMPT_LIMIT;

    @Value("file.manipulation.attempt.delay")
    private long FILE_MANIPULATION_ATTEMPT_DELAY;

    private final List<Shard> shards;

    HashBasedRequestManager(List<Shard> shards) {
        this.shards = shards;
    }

    @PostConstruct
    public void init() {
        LOG.trace("Initializing a Request Manager...");

        // initialize shards
        shards.stream().forEach(shard -> {
            shard.getFileSystem().init();
            shard.getDao().init();
        });
    }

    @Override
    public CommunicationChain onRequest(CommunicationChain communicationChain) {
        IRequest request = communicationChain.getRequest();

        IKey requestKey = request.getKey();
        int requestKeyHash = getHashCodeFrom(requestKey);

        // identify a shard
        Shard shard = shards.get(requestKeyHash % shards.size());

        if (request instanceof PutRequest) {
            PutRequest putRequest = (PutRequest) request;

            // business process
            Timestamp timestamp = new Timestamp(Instant.now().toEpochMilli());
            String filePath = shard.getPath() + requestKey.get() + UUID.randomUUID() + ".data";

            boolean isUpdatingRecordAdded = shard.getDao().addUpdatingEntry(timestamp, requestKey, filePath);
            if ( !isUpdatingRecordAdded ) {
                LOG.error("Unable to create a write ahead log record for the '{}' key.", requestKey.get());
                return communicationChain.withEmptyResponse();
            }

            boolean lockCreated = false;
            int numberOfAttempts = 0;
            while (lockCreated || numberOfAttempts == FILE_MANIPULATION_ATTEMPT_LIMIT) {
                lockCreated = shard.getFileSystem().lockFile(Paths.get(filePath));
                if ( !lockCreated ) {
                    numberOfAttempts++;
                    try {
                        Thread.sleep(FILE_MANIPULATION_ATTEMPT_DELAY);
                    }
                    catch (InterruptedException e) {
                        LOG.warn("A waiting thread for locking a file for the '{}' key is interrupted.", requestKey.get());
                        return communicationChain.withEmptyResponse();
                    }
                }
            }

            if ( !lockCreated ) {
                LOG.error("Unable to lock a file for the '{}' key.", requestKey.get());
                return communicationChain.withEmptyResponse();
            }

            boolean newFileAdded = shard.getFileSystem().createNewFileWithValue(Paths.get(filePath), putRequest.getValue());
            if ( !newFileAdded ) {
                LOG.error("Unable to create a new file for the '{}' key.", requestKey.get());
                return communicationChain.withEmptyResponse();
            }

            boolean entryCommitted = shard.getDao().commitEntry(timestamp, requestKey);
            if ( !entryCommitted ) {
                LOG.error("Unable to commit a white ahead log record for the '{}' key.", requestKey.get());
                return communicationChain.withEmptyResponse();
            }

            boolean fileUnlocked = shard.getFileSystem().unlockFile(Paths.get(filePath));
            if ( !fileUnlocked ) {
                LOG.error("Unable to unlock a file for the '{}' key.", requestKey.get());
                return communicationChain.withEmptyResponse();
            }

            return communicationChain.withSuccessResponse();
        }
        else if (request instanceof GetRequest) {
            Optional<String> filePath = shard.getDao().getCommittedPathFor(requestKey);
            if (filePath.isPresent()) {
                IValue value = shard.getFileSystem().getValueFrom(Paths.get(filePath.get()));
                return communicationChain.withValueResponse(value);
            }
            else {
                return communicationChain.withEmptyResponse();
            }
        }
        else if (request instanceof RemoveRequest) {
            Optional<String> filePath = shard.getDao().getCommittedPathFor(requestKey);
            if ( !filePath.isPresent() ) {
                return communicationChain.withSuccessResponse();
            }

            boolean lockCreated = false;
            int numberOfAttempts = 0;
            while (lockCreated || numberOfAttempts == FILE_MANIPULATION_ATTEMPT_LIMIT) {
                lockCreated = shard.getFileSystem().lockFile(Paths.get(filePath.get()));
                if ( !lockCreated ) {
                    numberOfAttempts++;
                    try {
                        Thread.sleep(FILE_MANIPULATION_ATTEMPT_DELAY);
                    }
                    catch (InterruptedException e) {
                        LOG.warn("A waiting thread for adding a value-file for the '{}' key is interrupted.", requestKey.get());
                        return communicationChain.withEmptyResponse();
                    }
                }
            }
            shard.getFileSystem().removeAllFilesWithMask(filePath.get());

        }
    }

    private int getHashCodeFrom(IKey key) {
        return key.get().hashCode();
    }
}
