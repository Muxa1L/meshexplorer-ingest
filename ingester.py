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
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass

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


class ClickHouseManager:
    """Manages ClickHouse database connections and operations."""
    
    def __init__(self, config: Config):
        self.config = config
        self._client: Optional[ClickHouseClient] = None
    
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
                logger.info(f"Connected to ClickHouse at {self.config.clickhouse_host}:{self.config.clickhouse_port}")
            except Exception as e:
                logger.error(f"Failed to connect to ClickHouse: {e}")
                raise
        return self._client
    
    def close(self):
        """Close ClickHouse connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("ClickHouse connection closed")
    
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
            
            client = self.get_client()
            
            columns = [
                'mesh_timestamp', 'broker', 'topic', 'origin', 'origin_pubkey',
                'message_hash', 'packet', 'payload', 'route_type', 'payload_type',
                'payload_version', 'header', 'path_len'
            ]
            
            broker_url = f"tcp://{self.config.mqtt_host}:{self.config.mqtt_port}"
            
            data = [
                packet_data['timestamp'],
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
            
            client = self.get_client()
            
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


class MeshCoreIngester:
    """Main ingester class that handles MQTT subscriptions and message processing."""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_manager = ClickHouseManager(config)
        self.mqtt_client: Optional[mqtt.Client] = None
        self._running = False
    
    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict, 
                    reason_code: int, properties: Any):
        """Callback for when the client receives a CONNACK response from the server."""
        if reason_code == 0:
            logger.info(f"Connected to MQTT broker at {self.config.mqtt_host}:{self.config.mqtt_port}")
            
            # Subscribe to topics
            topic = f"{self.config.mqtt_topic}/+/raw"
            client.subscribe(topic)
            logger.info(f"Subscribed to topic: {topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker, reason code: {reason_code}")
    
    def _on_disconnect(self, client: mqtt.Client, userdata: Any, reason_code: int, disconnect_flags: Any):
        """Callback for when the client disconnects from the broker."""
        if reason_code != 0:
            logger.warning(f"Unexpected disconnect from MQTT broker, reason code: {reason_code}")
        else:
            logger.info("Disconnected from MQTT broker")
    
    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        """Callback for when a PUBLISH message is received from the server."""
        try:
            data = json.loads(msg.payload)
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
            
            if self.mqtt_client:
                self.mqtt_client.disconnect()
                self.mqtt_client.loop_stop()
            
            self.db_manager.close()
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
