public enum APIKeys {
    PRODUCE(0, "Produce request"),
    FETCH(1, "Fetch request"),
    OFFSET(2, "Offsets request"),
    METADATA(3, "Metadata request"),
    LEADER_AND_ISR(4, "LeaderAndIsr request"),
    STOP_REPLICA(5, "StopReplica request"),
    UPDATE_METADATA(6, "UpdateMetadata request"),
    CONTROLLED_SHUTDOWN(7, "ControlledShutdown request"),
    OFFSET_COMMIT(8, "OffsetCommit request"),
    OFFSET_FETCH(9, "OffsetFetch request"),
    GROUP_COORDINATOR(10, "GroupCoordinator request"),
    JOIN_GROUP(11, "JoinGroup request"),
    HEARTBEAT(12, "Heartbeat request"),
    LEAVE_GROUP(13, "LeaveGroup request"),
    SYNC_GROUP(14, "SyncGroup request"),
    DESCRIBE_GROUPS(15, "DescribeGroups request"),
    LIST_GROUPS(16, "ListGroups request"),
    SASL_HANDSHAKE(17, "SaslHandshake request"),
    API_VERSIONS(18, "ApiVersions request");

    private final int code;
    private final String description;

    APIKeys(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return this.code;
    }

    public String getDescription() {
        return this.description;
    }

    public static APIKeys fromApiKey(short apiKey) {
        for (APIKeys key : APIKeys.values()) {
            if (key.getCode() == apiKey) {
                return key;
            }
        }
	// return null if something is wrong with the APIKEY
        return null; // o maybe implement exceptions
    }
}

