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
    CREATE_TOPICS(19, "CreateTopics request"),
    DELETE_TOPICS(20, "DeleteTopics request"),
    DELETE_RECORDS(21, "DeleteRecords request"),
    INIT_PRODUCER_ID(22, "InitProducerId request"),
    OFFSET_FOR_LEADER_EPOCH(23, "OffsetForLeaderEpoch request"),
    ADD_PARTITIONS_TO_TXN(24, "AddPartitionsToTxn request"),
    ADD_OFFSETS_TO_TXN(25, "AddOffsetsToTxn request"),
    END_TXN(26, "EndTxn request"),
    WRITE_TXN_MARKERS(27, "WriteTxnMarkers request"),
    TXN_OFFSET_COMMIT(28, "TxnOffsetCommit request"),
    DESCRIBE_ACLS(29, "DescribeAcls request"),
    CREATE_ACLS(30, "CreateAcls request"),
    DELETE_ACLS(31, "DeleteAcls request"),
    DESCRIBE_CONFIGS(32, "DescribeConfigs request"),
    ALTER_CONFIGS(33, "AlterConfigs request"),
    ALTER_REPLICA_LOG_DIRS(34, "AlterReplicaLogDirs request"),
    DESCRIBE_LOG_DIRS(35, "DescribeLogDirs request"),
    SASL_AUTHENTICATE(36, "SaslAuthenticate request"),
    CREATE_PARTITIONS(37, "CreatePartitions request"),
    CREATE_DELEGATION_TOKEN(38, "CreateDelegationToken request"),
    RENEW_DELEGATION_TOKEN(39, "RenewDelegationToken request"),
    EXPIRE_DELEGATION_TOKEN(40, "ExpireDelegationToken request"),
    DESCRIBE_DELEGATION_TOKEN(41, "DescribeDelegationToken request"),
    DELETE_GROUPS(42, "DeleteGroups request"),
    ELECT_LEADERS(43, "ElectLeaders request"),
    INCREMENTAL_ALTER_CONFIGS(44, "IncrementalAlterConfigs request"),
    ALTER_PARTITION_REASSIGNMENTS(45, "AlterPartitionReassignments request"),
    LIST_PARTITION_REASSIGNMENTS(46, "ListPartitionReassignments request"),
    OFFSET_DELETE(47, "OffsetDelete request"),
    DESCRIBE_CLIENT_QUOTAS(48, "DescribeClientQuotas request"),
    ALTER_CLIENT_QUOTAS(49, "AlterClientQuotas request"),
    DESCRIBE_USER_SCRAM_CREDENTIALS(50, "DescribeUserScramCredentials request"),
    ALTER_USER_SCRAM_CREDENTIALS(51, "AlterUserScramCredentials request"),
    DESCRIBE_QUORUM(55, "DescribeQuorum request"),
    ALTER_PARTITION(56, "AlterPartition request"),
    UPDATE_FEATURES(57, "UpdateFeatures request"),
    ENVELOPE(58, "Envelope request"),
    DESCRIBE_CLUSTER(60, "DescribeCluster request"),
    DESCRIBE_PRODUCERS(61, "DescribeProducers request"),
    UNREGISTER_BROKER(64, "UnregisterBroker request"),
    DESCRIBE_TRANSACTIONS(65, "DescribeTransactions request"),
    LIST_TRANSACTIONS(66, "ListTransactions request"),
    ALLOCATE_PRODUCER_IDS(67, "AllocateProducerIds request"),
    CONSUMER_GROUP_HEARTBEAT(68, "ConsumerGroupHeartbeat request"),
    CONSUMER_GROUP_DESCRIBE(69, "ConsumerGroupDescribe request"),
    GET_TELEMETRY_SUBSCRIPTIONS(71, "GetTelemetrySubscriptions request"),
    PUSH_TELEMETRY(72, "PushTelemetry request"),
    LIST_CLIENT_METRICS_RESOURCES(74, "ListClientMetricsResources request"),
    DESCRIBE_TOPIC_PARTITIONS(75, "DescribeTopicPartitions request");

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

