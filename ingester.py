#!/usr/bin/env python3
"""
MeshExplorer Ingester
Subscribes to MQTT topics and ingests MeshCore packets into ClickHouse database.
"""

import os
import sys
import json
import logging
import binascii
import sqlite3
import threading
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path

import paho.mqtt.client as mqtt
import clickhouse_connect
from clickhouse_connect.driver import Client as ClickHouseClient

from meshcoredecoder import MeshCoreDecoder
from meshcoredecoder.types.enums import PayloadType


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Application configuration loaded from environment variables."""
    
    # ClickHouse settings
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_pass: str
    
    # MQTT settings
    mqtt_host: str
    mqtt_port: int
    mqtt_user: str
    mqtt_pass: str
    mqtt_topic: str
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables."""
        try:
            return cls(
                clickhouse_host=os.environ['CLICKHOUSE_HOST'],
                clickhouse_port=int(os.environ['CLICKHOUSE_PORT']),
                clickhouse_user=os.environ['CLICKHOUSE_USER'],
                clickhouse_pass=os.environ['CLICKHOUSE_PASS'],
                mqtt_host=os.environ['MQTT_HOST'],
                mqtt_port=int(os.environ['MQTT_PORT']),
                mqtt_user=os.environ['MQTT_USER'],
                mqtt_pass=os.environ['MQTT_PASS'],
                mqtt_topic=os.environ['MQTT_TOPIC']
            )
        except KeyError as e:
            logger.error(f"Missing required environment variable: {e}")
            raise
        except ValueError as e:
            logger.error(f"Invalid environment variable value: {e}")
            raise


class PersistentQueue:
    """SQLite-based persistent queue for storing messages when ClickHouse is unavailable."""
    
    def __init__(self, db_path: str = "queue.db"):
        """Initialize the persistent queue."""
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()
        logger.info(f"Initialized persistent queue at {db_path}")
    
    def _init_db(self):
        """Initialize the SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS message_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON message_queue(created_at)")
            conn.commit()
    
    def enqueue(self, message_type: str, payload: Dict[str, Any]) -> bool:
        """
        Add a message to the queue.
        
        Args:
            message_type: Type of message ('packet', 'advert', 'status')
            payload: Message payload as dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        "INSERT INTO message_queue (message_type, payload) VALUES (?, ?)",
                        (message_type, json.dumps(payload))
                    )
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to enqueue message: {e}")
            return False
    
    def dequeue_batch(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Get a batch of messages from the queue.
        
        Args:
            batch_size: Maximum number of messages to retrieve
            
        Returns:
            List of messages with id, message_type, and payload
        """
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute(
                        "SELECT id, message_type, payload FROM message_queue ORDER BY id LIMIT ?",
                        (batch_size,)
                    )
                    rows = cursor.fetchall()
                    
                    messages = []
                    for row in rows:
                        messages.append({
                            'id': row[0],
                            'message_type': row[1],
                            'payload': json.loads(row[2])
                        })
                    return messages
        except Exception as e:
            logger.error(f"Failed to dequeue messages: {e}")
            return []
    
    def delete_messages(self, message_ids: List[int]) -> bool:
        """
        Delete messages from the queue by their IDs.
        
        Args:
            message_ids: List of message IDs to delete
            
        Returns:
            True if successful, False otherwise
        """
        if not message_ids:
            return True
            
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    placeholders = ','.join('?' * len(message_ids))
                    conn.execute(
                        f"DELETE FROM message_queue WHERE id IN ({placeholders})",
                        message_ids
                    )
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to delete messages: {e}")
            return False
    
    def get_queue_size(self) -> int:
        """Get the current size of the queue."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM message_queue")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get queue size: {e}")
            return 0
    
    def close(self):
        """Clean up resources."""
        pass  # SQLite connections are managed per-operation


class ClickHouseManager:
    """Manages ClickHouse database connections and operations."""
    
    def __init__(self, config: Config, queue: PersistentQueue):
        self.config = config
        self.queue = queue
        self._client: Optional[ClickHouseClient] = None
        self._clickhouse_available = True
    
    def get_client(self) -> ClickHouseClient:
        """Get or create ClickHouse client connection."""
        if self._client is None:
            try:
                self._client = clickhouse_connect.get_client(
                    host=self.config.clickhouse_host,
                    port=self.config.clickhouse_port,
                    username=self.config.clickhouse_user,
                    password=self.config.clickhouse_pass
                )
                self._client.command('SELECT 1')
                logger.info(f"Connected to ClickHouse at {self.config.clickhouse_host}:{self.config.clickhouse_port}")
                self._clickhouse_available = True
            except Exception as e:
                logger.error(f"Failed to connect to ClickHouse: {e}")
                self._clickhouse_available = False
                raise
        return self._client
    
    def is_available(self) -> bool:
        """Check if ClickHouse is available."""
        return self._clickhouse_available
    
    def close(self):
        """Close ClickHouse connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("ClickHouse connection closed")
    
    def flush_queue(self) -> int:
        """
        Flush queued messages to ClickHouse.
        
        Returns:
            Number of messages successfully processed
        """
        if not self.is_available():
            return 0
            
        messages = self.queue.dequeue_batch(batch_size=100)
        if not messages:
            return 0
        
        processed_ids = []
        for msg in messages:
            success = False
            msg_type = msg['message_type']
            payload = msg['payload']
            
            try:
                if msg_type == 'packet':
                    success = self.insert_packet(payload)
                elif msg_type == 'advert':
                    success = self.insert_advert(payload)
                elif msg_type == 'status':
                    success = self.insert_status(payload)
                
                if success:
                    processed_ids.append(msg['id'])
            except Exception as e:
                logger.error(f"Failed to process queued message {msg['id']}: {e}")
        
        if processed_ids:
            self.queue.delete_messages(processed_ids)
            logger.info(f"Flushed {len(processed_ids)} messages from queue")
        
        return len(processed_ids)
    
    def insert_packet(self, packet_data: Dict[str, Any]) -> bool:
        """
        Insert a decoded packet into the meshcore_packets table.
        
        Args:
            packet_data: Raw packet data from MQTT
            
        Returns:
            True if successful, False otherwise
        """
        try:
            packet = MeshCoreDecoder.decode(packet_data['data']).to_dict()
            
            if not packet['isValid']:
                logger.warning(f"Invalid packet received: {packet_data.get('origin', 'unknown')}")
                return False
            
            try:
                client = self.get_client()
            except Exception:
                # ClickHouse unavailable, queue the message
                logger.warning("ClickHouse unavailable, queueing packet message")
                return self.queue.enqueue('packet', packet_data)
            
            columns = [
                'mesh_timestamp', 'broker', 'topic', 'origin', 'origin_pubkey',
                'message_hash', 'packet', 'payload', 'route_type', 'payload_type',
                'payload_version', 'header', 'path_len'
            ]
            
            broker_url = f"tcp://{self.config.mqtt_host}:{self.config.mqtt_port}"
            
            data = [
                packet_data['timestamp']+'+00',
                broker_url,
                self.config.mqtt_topic.lower(),
                packet_data['origin'],
                binascii.unhexlify(packet_data['origin_id']),
                packet['messageHash'],
                binascii.unhexlify(packet_data['data']),
                binascii.unhexlify(packet['payload']['raw']),
                packet['routeType'],
                packet['payloadType'],
                packet['payloadVersion'],
                packet_data['data'][:2],  # header
                packet['pathLength']
            ]
            
            # Add path if present
            if packet['pathLength'] > 0:
                columns.append('path')
                data.append(binascii.unhexlify(''.join(packet['path'])))
            
            client.insert('meshcore_packets', [data], column_names=columns)
            logger.debug(f"Inserted packet: {packet['messageHash']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert packet: {e}", exc_info=True)
            return False
    
    def insert_advert(self, packet_data: Dict[str, Any]) -> bool:
        """
        Insert a decoded advert into the meshcore_adverts table.
        
        Args:
            packet_data: Raw packet data from MQTT
            
        Returns:
            True if successful, False otherwise
        """
        try:
            advert = MeshCoreDecoder.decode(packet_data['data']).to_dict()
            
            if not advert['isValid']:
                logger.warning(f"Invalid advert received: {packet_data.get('origin', 'unknown')}")
                return False
            
            try:
                client = self.get_client()
            except Exception:
                # ClickHouse unavailable, queue the message
                logger.warning("ClickHouse unavailable, queueing advert message")
                return self.queue.enqueue('advert', packet_data)
            
            payload_decoded = advert['payload']['decoded']
            app_data = payload_decoded['appData']
            
            columns = ['mesh_timestamp', 'adv_timestamp', 'public_key', 'node_name', 'has_location']
            data = [
                payload_decoded['timestamp'] * 1000,
                payload_decoded['timestamp'] * 1000,
                payload_decoded['publicKey'],
                app_data['name'],
                app_data['hasLocation']
            ]
            
            # Add location if present
            if app_data['hasLocation'] and app_data.get('location'):
                columns.extend(['latitude', 'longitude', 'altitude'])
                location = app_data['location']
                data.extend([
                    location['latitude'],
                    location['longitude'],
                    location.get('altitude', 0)
                ])
            
            # Add device role flags
            device_role = app_data.get('deviceRole', 0)
            if device_role == 1:
                columns.append('is_chat_node')
                data.append(True)
            elif device_role == 2:
                columns.append('is_repeater')
                data.append(True)
            elif device_role == 3:
                columns.append('is_room_server')
                data.append(True)
            
            # Add remaining fields
            broker_url = f"tcp://{self.config.mqtt_host}:{self.config.mqtt_port}"
            columns.extend(['has_name', 'broker', 'topic'])
            data.extend([
                app_data['hasName'],
                broker_url,
                self.config.mqtt_topic.lower()
            ])
            
            # Add path if present
            if advert['pathLength'] > 0:
                columns.append('path')
                data.append(binascii.unhexlify(''.join(advert['path'])))
            
            # Add final fields
            columns.extend(['path_len', 'origin_pubkey', 'origin', 'packet_hash'])
            data.extend([
                advert['pathLength'],
                binascii.unhexlify(packet_data['origin_id']),
                packet_data['origin'],
                binascii.unhexlify(advert['messageHash'])
            ])
            
            client.insert('meshcore_adverts', [data], column_names=columns)
            logger.info(f"Inserted advert from: {app_data['name']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert advert: {e}", exc_info=True)
            return False
    
    def insert_status(self, status_data: Dict[str, Any]) -> bool:
        """
        Insert node status data into the meshcore_status table.
        
        Args:
            status_data: Status data from MQTT
            
        Returns:
            True if successful, False otherwise
        """
        try:
            try:
                client = self.get_client()
            except Exception:
                # ClickHouse unavailable, queue the message
                logger.warning("ClickHouse unavailable, queueing status message")
                return self.queue.enqueue('status', status_data)
            
            stats = status_data.get('stats', {})
            broker_url = f"tcp://{self.config.mqtt_host}:{self.config.mqtt_port}"
            
            columns = [
                'timestamp', 'broker', 'topic', 'origin', 'origin_pubkey',
                'status', 'model', 'firmware_version', 'radio', 'client_version'
            ]
            
            data = [
                status_data['timestamp']+'+00',
                broker_url,
                self.config.mqtt_topic.lower(),
                status_data['origin'],
                binascii.unhexlify(status_data['origin_id'])
            ]
            
            # Add basic status fields
            data.extend([
                status_data.get('status', 'unknown'),
                status_data.get('model', ''),
                status_data.get('firmware_version', ''),
                status_data.get('radio', ''),
                status_data.get('client_version', '')
            ])
            
            # Add stats if present
            if stats:
                if 'battery_mv' in stats:
                    columns.append('battery_mv')
                    data.append(stats['battery_mv'])
                
                if 'uptime_secs' in stats:
                    columns.append('uptime_secs')
                    data.append(stats['uptime_secs'])
                
                if 'errors' in stats:
                    columns.append('errors')
                    data.append(stats['errors'])
                
                if 'queue_len' in stats:
                    columns.append('queue_len')
                    data.append(stats['queue_len'])
                
                if 'noise_floor' in stats:
                    columns.append('noise_floor')
                    data.append(stats['noise_floor'])
                
                if 'last_rssi' in stats:
                    columns.append('last_rssi')
                    data.append(stats['last_rssi'])
                
                if 'last_snr' in stats:
                    columns.append('last_snr')
                    data.append(stats['last_snr'])
                
                if 'tx_air_secs' in stats:
                    columns.append('tx_air_secs')
                    data.append(stats['tx_air_secs'])
                
                if 'rx_air_secs' in stats:
                    columns.append('rx_air_secs')
                    data.append(stats['rx_air_secs'])
            
            client.insert('meshcore_status', [data], column_names=columns)
            logger.info(f"Inserted status from: {status_data['origin']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert status: {e}", exc_info=True)
            return False


class MeshCoreIngester:
    """Main ingester class that handles MQTT subscriptions and message processing."""
    
    def __init__(self, config: Config):
        self.config = config
        self.queue = PersistentQueue()
        self.db_manager = ClickHouseManager(config, self.queue)
        self.mqtt_client: Optional[mqtt.Client] = None
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None
    
    def _flush_queue_loop(self):
        """Background thread that periodically flushes the queue."""
        while self._running:
            try:
                queue_size = self.queue.get_queue_size()
                if queue_size > 0:
                    logger.info(f"Queue size: {queue_size}, attempting to flush...")
                    flushed = self.db_manager.flush_queue()
                    if flushed == 0 and queue_size > 0:
                        logger.warning("Failed to flush queue, ClickHouse may be unavailable")
            except Exception as e:
                logger.error(f"Error in queue flush loop: {e}")
            
            # Sleep for 30 seconds before next flush attempt
            for _ in range(30):
                if not self._running:
                    break
                time.sleep(1)
    
    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict, 
                    reason_code: int, properties: Any):
        """Callback for when the client receives a CONNACK response from the server."""
        if reason_code == 0:
            logger.info(f"Connected to MQTT broker at {self.config.mqtt_host}:{self.config.mqtt_port}")
            
            # Subscribe to topics
            raw_topic = f"{self.config.mqtt_topic}/+/raw"
            client.subscribe(raw_topic)
            logger.info(f"Subscribed to topic: {raw_topic}")
            
            status_topic = f"{self.config.mqtt_topic}/+/status"
            client.subscribe(status_topic)
            logger.info(f"Subscribed to topic: {status_topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker, reason code: {reason_code}")
    
    def _on_disconnect(self, client: mqtt.Client, userdata: Any, reason_code: int, disconnect_flags: Any, properties: Any):
        """Callback for when the client disconnects from the broker."""
        if reason_code != 0:
            logger.warning(f"Unexpected disconnect from MQTT broker, reason code: {reason_code}")
        else:
            logger.info("Disconnected from MQTT broker")
    
    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        """Callback for when a PUBLISH message is received from the server."""
        try:
            data = json.loads(msg.payload)
            
            # Determine message type based on topic
            if msg.topic.endswith('/status'):
                # Handle status message
                self.db_manager.insert_status(data)
                
            elif msg.topic.endswith('/raw'):
                # Handle raw packet message
                hex_data = data.get('data')
                
                if not hex_data:
                    logger.warning(f"Received message without data field on topic {msg.topic}")
                    return
                
                # Decode packet to check type
                packet = MeshCoreDecoder.decode(hex_data)
                
                # Insert packet data
                self.db_manager.insert_packet(data)
                
                # Insert advert if present
                if packet.payload_type == PayloadType.Advert and packet.payload.get('decoded'):
                    self.db_manager.insert_advert(data)
            else:
                logger.warning(f"Received message on unknown subtopic: {msg.topic}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON message: {e}")
        except Exception as e:
            logger.error(f"Error processing message on topic {msg.topic}: {e}", exc_info=True)
    
    def start(self):
        """Start the ingester service."""
        try:
            # Initialize MQTT client
            self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            self.mqtt_client.on_connect = self._on_connect
            self.mqtt_client.on_disconnect = self._on_disconnect
            self.mqtt_client.on_message = self._on_message
            
            # Set credentials
            self.mqtt_client.username_pw_set(self.config.mqtt_user, self.config.mqtt_pass)
            
            # Connect to broker
            logger.info(f"Connecting to MQTT broker {self.config.mqtt_host}:{self.config.mqtt_port}...")
            self.mqtt_client.connect(self.config.mqtt_host, self.config.mqtt_port, 60)
            
            # Start the loop
            self._running = True
            
            # Start queue flushing thread
            self._flush_thread = threading.Thread(target=self._flush_queue_loop, daemon=True)
            self._flush_thread.start()
            logger.info("Started queue flushing thread")
            
            logger.info("MeshCore Ingester started. Press Ctrl+C to stop.")
            self.mqtt_client.loop_forever()
            
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
            self.stop()
        except Exception as e:
            logger.error(f"Fatal error in ingester: {e}", exc_info=True)
            self.stop()
            raise
    
    def stop(self):
        """Stop the ingester service gracefully."""
        if self._running:
            logger.info("Stopping MeshCore Ingester...")
            self._running = False
            
            # Wait for flush thread to finish
            if self._flush_thread and self._flush_thread.is_alive():
                logger.info("Waiting for queue flush thread to finish...")
                self._flush_thread.join(timeout=5)
            
            if self.mqtt_client:
                self.mqtt_client.disconnect()
                self.mqtt_client.loop_stop()
            
            self.db_manager.close()
            self.queue.close()
            logger.info("MeshCore Ingester stopped")


def main():
    """Main entry point for the application."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Create and start ingester
        ingester = MeshCoreIngester(config)
        ingester.start()
        
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Application failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
