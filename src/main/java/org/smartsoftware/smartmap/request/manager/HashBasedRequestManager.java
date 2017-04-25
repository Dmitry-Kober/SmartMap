package org.smartsoftware.smartmap.request.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.domain.communication.CommunicationChain;
import org.smartsoftware.smartmap.domain.communication.request.*;
import org.smartsoftware.smartmap.domain.data.IKey;
import org.smartsoftware.smartmap.domain.data.IValue;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

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

        scheduleHouseKeeper();

        shards.stream().forEach(shard -> {
            shard.getFileSystem().init();
            shard.getDao().init();
        });
    }

    private void scheduleHouseKeeper() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                performGC();
            }
        }, 1000, 2000);
    }

    private void performGC() {
        LOG.trace("A GC is started...");
        shards.stream().forEach(shard -> {
            Map<Integer, String> houseKeepingCandidates = shard.getDao().getAllRemovedAndCommittedEntriesButLatest();
            houseKeepingCandidates.values().stream().forEach(path -> shard.getFileSystem().removeFile(Paths.get(path)));
            shard.getDao().removeEntries(houseKeepingCandidates.keySet());

        });
    }

    @Override
    public CommunicationChain onRequest(CommunicationChain communicationChain) {
        IRequest request = communicationChain.getRequest();

        if (request instanceof PutRequest) {
            IKey requestKey = request.getKey();
            Shard shard = identifyShardFor(requestKey);
            LOG.trace("Processing a Put request for the '{}' key on the '{}' shard.", requestKey.get(), shard.getPath());

            return processPutRequest(communicationChain, requestKey, shard, (PutRequest) request);
        }
        else if (request instanceof GetRequest) {
            IKey requestKey = request.getKey();
            Shard shard = identifyShardFor(requestKey);
            LOG.trace("Processing a Get request for the '{}' key on the '{}' shard.", requestKey.get(), shard.getPath());

            return processGetRequest(communicationChain, requestKey, shard);
        }
        else if (request instanceof RemoveRequest) {
            IKey requestKey = request.getKey();
            Shard shard = identifyShardFor(requestKey);
            LOG.trace("Processing a Remove request for the '{}' key on the '{}' shard.", requestKey.get(), shard.getPath());

            return processRemoveRequest(communicationChain, requestKey, shard);
        }
        else if (request instanceof ListKeysRequest) {
            LOG.trace("Processing a ListKey request.");
            return processListKeysRequest(communicationChain);
        }
        else {
            return processUnknownRequest(communicationChain);
        }
    }

    private Shard identifyShardFor(IKey requestKey) {
        return shards.get(getHashCodeFrom(requestKey) % shards.size());
    }

    private CommunicationChain processUnknownRequest(CommunicationChain communicationChain) {
        LOG.error("Unknown request is received.");
        return communicationChain.withFailedResponse();
    }

    private CommunicationChain processListKeysRequest(CommunicationChain communicationChain) {
        List<String> allLatestCommittedKeys = new LinkedList<>();
        for (Shard shardItem : shards) {
            allLatestCommittedKeys.addAll(shardItem.getDao().getAllLatestCommittedKeys());
        }
        return communicationChain.withListResponse(allLatestCommittedKeys);
    }

    private CommunicationChain processRemoveRequest(CommunicationChain communicationChain, IKey requestKey, Shard shard) {
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

    private CommunicationChain processGetRequest(CommunicationChain communicationChain, IKey requestKey, Shard shard) {
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

    private CommunicationChain processPutRequest(CommunicationChain communicationChain, IKey requestKey, Shard shard, PutRequest putRequest) {
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

    private int getHashCodeFrom(IKey key) {
        return key.get().hashCode();
    }
}
