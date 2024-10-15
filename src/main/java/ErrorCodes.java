public enum ErrorCodes {
    UNKNOWN(-1, false, "The server experienced an unexpected error when processing the request"),
    NONE(0, false, ""),
    OFFSET_OUT_OF_RANGE(1, false, "The requested offset is not within the range of offsets maintained by the server."),
    CORRUPT_MESSAGE(2, true, "This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt."),
    UNKNOWN_TOPIC_OR_PARTITION(3, true, "This server does not host this topic-partition."),
    INVALID_FETCH_SIZE(4, false, "The requested fetch size is invalid."),
    LEADER_NOT_AVAILABLE(5, true, "There is no leader for this topic-partition as we are in the middle of a leadership election."),
    NOT_LEADER_FOR_PARTITION(6, true, "This server is not the leader for that topic-partition."),
    REQUEST_TIMED_OUT(7, true, "The request timed out."),
    BROKER_NOT_AVAILABLE(8, false, "The broker is not available."),
    REPLICA_NOT_AVAILABLE(9, false, "The replica is not available for the requested topic-partition."),
    MESSAGE_TOO_LARGE(10, false, "The request included a message larger than the max message size the server will accept."),
    STALE_CONTROLLER_EPOCH(11, false, "The controller moved to another broker."),
    OFFSET_METADATA_TOO_LARGE(12, false, "The metadata field of the offset request was too large."),
    NETWORK_EXCEPTION(13, true, "The server disconnected before a response was received."),
    GROUP_LOAD_IN_PROGRESS(14, true, "The coordinator is loading and hence can't process requests for this group."),
    GROUP_COORDINATOR_NOT_AVAILABLE(15, true, "The group coordinator is not available."),
    NOT_COORDINATOR_FOR_GROUP(16, true, "This is not the correct coordinator for this group."),
    INVALID_TOPIC_EXCEPTION(17, false, "The request attempted to perform an operation on an invalid topic."),
    RECORD_LIST_TOO_LARGE(18, false, "The request included message batch larger than the configured segment size on the server."),
    NOT_ENOUGH_REPLICAS(19, true, "Messages are rejected since there are fewer in-sync replicas than required."),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(20, true, "Messages are written to the log, but to fewer in-sync replicas than required."),
    INVALID_REQUIRED_ACKS(21, false, "Produce request specified an invalid value for required acks."),
    ILLEGAL_GENERATION(22, false, "Specified group generation id is not valid."),
    INCONSISTENT_GROUP_PROTOCOL(23, false, "The group member's supported protocols are incompatible with those of existing members."),
    INVALID_GROUP_ID(24, false, "The configured groupId is invalid."),
    UNKNOWN_MEMBER_ID(25, false, "The coordinator is not aware of this member."),
    INVALID_SESSION_TIMEOUT(26, false, "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)."),
    REBALANCE_IN_PROGRESS(27, false, "The group is rebalancing, so a rejoin is needed."),
    INVALID_COMMIT_OFFSET_SIZE(28, false, "The committing offset data size is not valid."),
    TOPIC_AUTHORIZATION_FAILED(29, false, "Not authorized to access topics: [Topic authorization failed.]"),
    GROUP_AUTHORIZATION_FAILED(30, false, "Not authorized to access group: Group authorization failed."),
    CLUSTER_AUTHORIZATION_FAILED(31, false, "Cluster authorization failed."),
    INVALID_TIMESTAMP(32, false, "The timestamp of the message is out of acceptable range."),
    UNSUPPORTED_SASL_MECHANISM(33, false, "The broker does not support the requested SASL mechanism."),
    ILLEGAL_SASL_STATE(34, false, "Request is not valid given the current SASL state."),
    UNSUPPORTED_VERSION(35, false, "The version of API is not supported.");

    private final int code;
    private final boolean retryable;
    private final String message;

    ErrorCodes(int code, boolean retryable, String message) {
        this.code = code;
        this.retryable = retryable;
        this.message = message;
    }

    public int getCode() {
        return this.code;
    }

    public boolean isRetryable() {
        return this.retryable;
    }

    public String getMessage() {
        return this.message;
    }

    public static ErrorCodes fromApiVersion(short version) {
        if (version < 0 || version > 4) {
            return UNSUPPORTED_VERSION;
        }
        return NONE;
    }
}

