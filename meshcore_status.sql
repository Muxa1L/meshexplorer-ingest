-- DDL for meshcore_status table
-- Stores node status information from MQTT /status subtopic

CREATE TABLE IF NOT EXISTS meshcore_status
(
    timestamp DateTime64(6) COMMENT 'Status timestamp from MQTT message',
    broker String COMMENT 'MQTT broker URL',
    topic LowCardinality(String) COMMENT 'MQTT topic name',
    origin String COMMENT 'Node origin identifier',
    origin_pubkey FixedString(32) COMMENT 'Node public key',
    status LowCardinality(String) COMMENT 'Node status (online/offline)',
    model String COMMENT 'Device model',
    firmware_version String COMMENT 'Firmware version',
    radio String COMMENT 'Radio configuration (frequency, bandwidth, SF, CR)',
    client_version String COMMENT 'Client software version',
    battery_mv Nullable(UInt32) COMMENT 'Battery voltage in millivolts',
    uptime_secs Nullable(UInt64) COMMENT 'Node uptime in seconds',
    errors Nullable(UInt32) COMMENT 'Number of errors',
    queue_len Nullable(UInt32) COMMENT 'Message queue length',
    noise_floor Nullable(Int16) COMMENT 'Radio noise floor in dBm',
    last_rssi Nullable(Int16) COMMENT 'Last RSSI in dBm',
    last_snr Nullable(Float32) COMMENT 'Last SNR in dB',
    tx_air_secs Nullable(UInt32) COMMENT 'Transmit air time in seconds',
    rx_air_secs Nullable(UInt32) COMMENT 'Receive air time in seconds'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (origin_pubkey, timestamp)
SETTINGS index_granularity = 8192;

-- Index for efficient queries by origin name
CREATE INDEX IF NOT EXISTS idx_origin ON meshcore_status (origin) TYPE bloom_filter(0.01) GRANULARITY 1;

-- Index for status queries
CREATE INDEX IF NOT EXISTS idx_status ON meshcore_status (status) TYPE set(0) GRANULARITY 1;
