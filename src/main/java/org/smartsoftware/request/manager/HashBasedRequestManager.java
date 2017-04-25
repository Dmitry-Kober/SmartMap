package org.smartsoftware.request.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.domain.communication.CommunicationChain;
import org.smartsoftware.domain.communication.request.GetRequest;
import org.smartsoftware.domain.communication.request.IRequest;
import org.smartsoftware.domain.communication.request.PutRequest;
import org.smartsoftware.domain.communication.request.RemoveRequest;
import org.smartsoftware.domain.data.IKey;
import org.smartsoftware.domain.data.IValue;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.file.Paths;
import java.sql.Timestamp;
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
            String filePath = shard.getPath() + "/" + requestKey.get() + "$" + UUID.randomUUID() + ".data";

            boolean isUpdatingRecordAdded = shard.getDao().addUpdatingEntry(timestamp, requestKey, filePath);
            if ( !isUpdatingRecordAdded ) {
                LOG.error("Unable to create a write ahead log record for the '{}' key.", requestKey.get());
                return communicationChain.withFailedResponse();
            }

            boolean newFileAdded = shard.getFileSystem().createNewFileWithValue(Paths.get(filePath), putRequest.getValue());
            if ( !newFileAdded ) {
                LOG.error("Unable to create a new file for the '{}' key.", requestKey.get());
                return communicationChain.withFailedResponse();
            }

            boolean entryCommitted = shard.getDao().commitEntry(timestamp, requestKey);
            if ( !entryCommitted ) {
                LOG.error("Unable to commit a white ahead log record for the '{}' key.", requestKey.get());
                return communicationChain.withFailedResponse();
            }


            return communicationChain.withSuccessResponse();
        }
        else if (request instanceof GetRequest) {
            Optional<String> filePath = shard.getDao().getCommittedPathFor(requestKey);
            if (filePath.isPresent()) {
                IValue value = shard.getFileSystem().getValueFrom(Paths.get(filePath.get()));
                if (value.get().isPresent()) {
                    return communicationChain.withValueResponse(value);
                }
                else {
                    return communicationChain.withEmptyResponse();
                }
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
            boolean entriesMarkedAsRemoved = shard.getDao().markEntriesAsRemoved(requestKey);
            if ( !entriesMarkedAsRemoved ) {
                LOG.error("Unable to mark entries as removed for the '{}' key.", requestKey.get());
                return communicationChain.withFailedResponse();
            }

            boolean filesAreRemoved = shard.getFileSystem().removeAllFilesWithMask(
                    Paths.get(shard.getPath()),
                    String.valueOf(requestKey.get()) + "\\$.*\\.data"
            );
            if ( !filesAreRemoved ) {
                LOG.error("Unable to remove files for the '{}' key.", requestKey.get());
                return communicationChain.withFailedResponse();
            }

            return communicationChain.withSuccessResponse();
        }
        else {
            LOG.error("Unknown request is received.");
            return communicationChain.withFailedResponse();
        }
    }

    private int getHashCodeFrom(IKey key) {
        return key.get().hashCode();
    }
}
