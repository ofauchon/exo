"""
This module provides a standalone implementation for Consul-based service discovery.
It allows nodes to register themselves with a Consul server and discover other nodes.
"""

import os
import json
import time
import socket
import logging
import uuid
import requests
import threading
import asyncio
from typing import Dict, List, Optional, Any, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConsulDiscovery:
    """
    Implements service discovery using Consul.
    Allows nodes to register with a Consul server and discover other peers.
    """
    
    def __init__(self, consul_url=None, service_name=None, node_id=None, ttl=None, check_interval=None, api_port=None):
        """
        Initialize the Consul discovery service.
        
        Args:
            consul_url: URL of the Consul server (default: http://localhost:8500)
            service_name: Name of the service to register (default: exo-node)
            node_id: Unique identifier for this node (default: generated UUID)
            ttl: Time-to-live for health checks in seconds (default: 30)
            check_interval: Interval for sending health updates in seconds (default: 10)
            api_port: Port for the node's API (default: 8080)
        """
        self.consul_url = consul_url or os.environ.get("CONSUL_URL", "http://localhost:8500")
        self.service_name = service_name or os.environ.get("CONSUL_SERVICE_NAME", "exo-node")
        self.node_id = node_id or os.environ.get("NODE_ID") or str(uuid.uuid4())
        self.check_interval = check_interval or int(os.environ.get("CONSUL_CHECK_INTERVAL", "10"))
        self.ttl = ttl or int(os.environ.get("CONSUL_TTL", "30"))
        self.metadata = {}
        self.registered = False
        self.running = False
        self._health_task = None
        
        # Get local address information
        self.hostname = socket.gethostname()
        self.ip_address = self._get_local_ip()
        self.api_port = api_port or int(os.environ.get("API_PORT", "8080"))
        
    def _get_local_ip(self) -> str:
        """Get the local IP address."""
        try:
            # Create a socket to determine the outgoing IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception as e:
            logger.warning(f"Could not determine IP address: {e}")
            return "127.0.0.1"
    
    async def _async_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Tuple[bool, Any]:
        """
        Make async HTTP requests to the Consul API.
        
        Args:
            method: HTTP method (GET, PUT, DELETE)
            endpoint: API endpoint
            data: Optional JSON data to send
            
        Returns:
            Tuple of (success, response_data)
        """
        url = f"{self.consul_url}/v1{endpoint}"
        headers = {'Content-Type': 'application/json'}
        
        try:
            # Use asyncio to run requests in a thread pool
            loop = asyncio.get_event_loop()
            if method == "GET":
                response = await loop.run_in_executor(
                    None, 
                    lambda: requests.get(url, headers=headers, timeout=5)
                )
            elif method == "PUT":
                response = await loop.run_in_executor(
                    None, 
                    lambda: requests.put(url, headers=headers, json=data, timeout=5)
                )
            elif method == "DELETE":
                response = await loop.run_in_executor(
                    None, 
                    lambda: requests.delete(url, headers=headers, timeout=5)
                )
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return False, None
                
            response.raise_for_status()
            if response.status_code == 204 or not response.text:
                return True, None
            return True, response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Consul API request failed: {e}")
            return False, None
    
    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Tuple[bool, Any]:
        """
        Make synchronous HTTP requests to the Consul API.
        
        Args:
            method: HTTP method (GET, PUT, DELETE)
            endpoint: API endpoint
            data: Optional JSON data to send
            
        Returns:
            Tuple of (success, response_data)
        """
        url = f"{self.consul_url}/v1{endpoint}"
        #headers = {'Content-Type': 'application/json'}
        headers = {}
        
        try:
            if method == "GET":
                response = requests.get(url, headers=headers, timeout=5)
            elif method == "PUT":
                response = requests.put(url, headers=headers, json=data, timeout=5)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers, timeout=5)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return False, None
                
            response.raise_for_status()
            if response.status_code == 204 or not response.text:
                return True, None
            return True, response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Consul API request failed: {e}")
            return False, None
    
    def set_metadata(self, metadata: Dict) -> None:
        """
        Set node metadata to be shared with the Consul service registry.
        
        Args:
            metadata: Dictionary of metadata to share
        """
        self.metadata = metadata
    
    async def async_register(self) -> bool:
        """
        Register this node with the Consul service discovery asynchronously.
        
        Returns:
            bool: True if registration was successful
        """
        service_id = f"{self.service_name}-{self.node_id}"
        
        # Prepare service registration payload
        service_data = {
            "ID": service_id,
            "Name": self.service_name,
            "Address": self.ip_address,
            "Port": self.api_port,
            "Check": {
                "TTL": f"{self.ttl}s",
                "DeregisterCriticalServiceAfter": "1m"
            },
            "Meta": {
                "node_id": self.node_id,
                "hostname": self.hostname,
                **self.metadata
            }
        }
        
        success, _ = await self._async_request("PUT", f"/agent/service/register", service_data)
        
        if success:
            logger.info(f"Registered with Consul as {service_id}")
            self.registered = True
            # Initial check-in
            await self._async_update_health_check()
            return True
        else:
            logger.error("Failed to register with Consul")
            return False
    
    def register(self) -> bool:
        """
        Register this node with the Consul service discovery.
        
        Returns:
            bool: True if registration was successful
        """
        service_id = f"{self.service_name}-{self.node_id}"
        
        # Prepare service registration payload
        service_data = {
            "ID": service_id,
            "Name": self.service_name,
            "Address": self.ip_address,
            "Port": self.api_port,
            "Check": {
                "TTL": f"{self.ttl}s",
                "DeregisterCriticalServiceAfter": "1m"
            },
            "Meta": {
                "node_id": self.node_id,
                "hostname": self.hostname,
                **self.metadata
            }
        }
        
        success, _ = self._request("PUT", f"/agent/service/register", service_data)
        
        if success:
            logger.info(f"Registered with Consul as {service_id}")
            self.registered = True
            # Initial check-in
            self._update_health_check()
            return True
        else:
            logger.error("Failed to register with Consul")
            return False
    
    async def _async_update_health_check(self) -> bool:
        """Update the TTL health check to keep the service registered (async version)."""
        service_id = f"{self.service_name}-{self.node_id}"
        success, _ = await self._async_request("PUT", f"/agent/check/pass/service:{service_id}")
        
        if not success:
            logger.warning("Failed to update health check")
        
        return success
    
    def _update_health_check(self) -> bool:
        """Update the TTL health check to keep the service registered."""
        service_id = f"{self.service_name}-{self.node_id}"
        success, _ = self._request("PUT", f"/agent/check/pass/service:{service_id}")
        
        if not success:
            logger.warning("Failed to update health check")
        
        return success
    
    async def async_deregister(self) -> bool:
        """
        Deregister this node from Consul service discovery asynchronously.
        
        Returns:
            bool: True if deregistration was successful
        """
        if not self.registered:
            return True
            
        service_id = f"{self.service_name}-{self.node_id}"
        success, _ = await self._async_request("PUT", f"/agent/service/deregister/{service_id}")
        
        if success:
            logger.info(f"Deregistered from Consul: {service_id}")
            self.registered = False
            return True
        else:
            logger.error("Failed to deregister from Consul")
            return False
    
    def deregister(self) -> bool:
        """
        Deregister this node from Consul service discovery.
        
        Returns:
            bool: True if deregistration was successful
        """
        if not self.registered:
            return True
            
        service_id = f"{self.service_name}-{self.node_id}"
        success, _ = self._request("PUT", f"/agent/service/deregister/{service_id}")
        
        if success:
            logger.info(f"Deregistered from Consul: {service_id}")
            self.registered = False
            return True
        else:
            logger.error("Failed to deregister from Consul")
            return False
    
    async def async_discover_peers(self) -> List[Dict]:
        """
        Discover other nodes registered with the same service name asynchronously.
        
        Returns:
            List of dictionaries containing peer information
        """
        success, data = await self._async_request("GET", f"/catalog/service/{self.service_name}")
        
        if not success or not data:
            logger.warning("Failed to discover peers or no peers found")
            return []
        
        peers = []
        for service in data:
            # Skip ourselves
            if service.get("ServiceMeta", {}).get("node_id") == self.node_id:
                continue
                
            peer = {
                "id": service.get("ServiceMeta", {}).get("node_id"),
                "hostname": service.get("ServiceMeta", {}).get("hostname"),
                "address": service.get("ServiceAddress"),
                "port": service.get("ServicePort"),
                "metadata": {k.replace("ServiceMeta_", ""): v for k, v in service.items() if k.startswith("ServiceMeta_")}
            }
            peers.append(peer)
            
        return peers
    
    async def discover_peers(self,  wait_for_peers: int = 0) -> List[Dict]:
        """
        Discover other nodes registered with the same service name.
        
        Returns:
            List of dictionaries containing peer information
        """
        success, data = self._request("GET", f"/catalog/service/{self.service_name}")
        
        if not success or not data:
            logger.warning("Failed to discover peers or no peers found")
            return []
        
        #logger.info(data)

        peers = []
        for service in data:
            # Skip ourselves
            if service.get("ServiceMeta", {}).get("node_id") == self.node_id:
                logger.info(f"XXXXXX Skip me {self.service_name}")
                continue

            logger.info(service)

            peer = {
                "id": service.get("ServiceMeta", {}).get("node_id"),
                "hostname": service.get("ServiceMeta", {}).get("hostname"),
                "address": service.get("ServiceAddress"),
                "port": service.get("ServicePort"),
                "metadata": {k.replace("ServiceMeta_", ""): v for k, v in service.items() if k.startswith("ServiceMeta_")}
            }
            logger.info(f"XXXXXX {peer}")


            peers.append(peer)
            
        return peers
    
    async def _health_updater_coro(self):
        """Asynchronous coroutine for health check updates."""
        logger.info(f"Async health check updater started (interval: {self.check_interval}s)")
        
        while self.running and self.registered:
            await asyncio.sleep(self.check_interval)
            await self._async_update_health_check()
            
        logger.info("Async health check updater stopped")
    
    async def start(self):
        """
        Start the Consul discovery service asynchronously.
        This registers the service and starts the health check updater.
        
        Returns:
            bool: True if successfully started
        """
        if self.running:
            logger.warning("Service is already running")
            return False
            
        # Register with Consul
        if not await self.async_register():
            logger.error("Failed to start: registration failed")
            return False
            
        # Start health check updater as an asyncio task
        self.running = True
        self._health_task = asyncio.create_task(self._health_updater_coro())
        
        logger.info("Consul discovery service started")
        return True
    
    async def stop(self):
        """
        Stop the Consul discovery service asynchronously.
        This stops the health check updater and deregisters the service.
        
        Returns:
            bool: True if successfully stopped
        """
        if not self.running:
            logger.warning("Service is not running")
            return True
            
        # Stop the health updater task
        self.running = False
        if self._health_task:
            try:
                # Wait for the task to complete
                await asyncio.wait_for(self._health_task, timeout=2)
            except asyncio.TimeoutError:
                # Cancel the task if it doesn't complete in time
                self._health_task.cancel()
                
        # Deregister from Consul
        success = await self.async_deregister()
        
        logger.info("Consul discovery service stopped")
        return success
    
    def _health_updater_thread(self):
        """Background thread that periodically updates the health check."""
        logger.info(f"Health check updater started (interval: {self.check_interval}s)")
        
        while self.running and self.registered:
            time.sleep(self.check_interval)
            self._update_health_check()
            
        logger.info("Health check updater stopped")
    
    def start_sync(self):
        """
        Start the Consul discovery service synchronously.
        This registers the service and starts the health check updater.
        
        Returns:
            bool: True if successfully started
        """
        if self.running:
            logger.warning("Service is already running")
            return False
            
        # Register with Consul
        if not self.register():
            logger.error("Failed to start: registration failed")
            return False
            
        # Start health check updater in background thread
        self.running = True
        self._health_thread = threading.Thread(target=self._health_updater_thread, daemon=True)
        self._health_thread.start()
        
        logger.info("Consul discovery service started")
        return True
    
    def stop_sync(self):
        """
        Stop the Consul discovery service synchronously.
        This stops the health check updater and deregisters the service.
        
        Returns:
            bool: True if successfully stopped
        """
        if not self.running:
            logger.warning("Service is not running")
            return True
            
        # Stop the health updater thread
        self.running = False
        if hasattr(self, '_health_thread') and self._health_thread and self._health_thread.is_alive():
            self._health_thread.join(timeout=2)
            
        # Deregister from Consul
        success = self.deregister()
        
        logger.info("Consul discovery service stopped")
        return success
    
    def start_health_updater(self):
        """
        Legacy method for compatibility. 
        Starts a blocking health updater (not in a background thread).
        """
        if not self.registered:
            logger.warning("Cannot start health updater: service not registered yet")
            return
            
        try:
            logger.info(f"Starting health check updater (interval: {self.check_interval}s)")
            while self.registered:
                time.sleep(self.check_interval)
                self._update_health_check()
        except KeyboardInterrupt:
            logger.info("Health updater interrupted")
            self.deregister()

# Example async usage
async def async_main():
    """Example async usage of Consul discovery."""
    # Create the discovery service
    consul = ConsulDiscovery(
        consul_url="http://localhost:8500",
        service_name="exo-node"
    )
    
    # Set metadata about this node
    consul.set_metadata({
        "version": "1.0.0",
        "capabilities": json.dumps(["compute", "storage"])
    })
    
    # Start the service (registers and starts health updater)
    if await consul.start():
        try:
            # Discover peers
            peers = await consul.async_discover_peers()
            print(f"Discovered {len(peers)} peers: {peers}")
            
            # Keep the main coroutine running
            print("Service running. Press Ctrl+C to stop...")
            try:
                # Run forever until interrupted
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                print("Async task cancelled")
                
        finally:
            # Stop the service
            await consul.stop()

# Example sync usage
def main():
    """Example synchronous usage of Consul discovery."""
    # Create the discovery service
    consul = ConsulDiscovery(
        consul_url="http://localhost:8500",
        service_name="exo-node"
    )
    
    # Set metadata about this node
    consul.set_metadata({
        "version": "1.0.0",
        "capabilities": json.dumps(["compute", "storage"])
    })
    
    # Start the service synchronously
    if consul.start_sync():
        try:
            # Discover peers
            peers = consul.discover_peers()
            print(f"Discovered {len(peers)} peers: {peers}")
            
            # Keep the main thread running
            print("Service running. Press Ctrl+C to stop...")
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            # Stop the service
            consul.stop_sync()
    
if __name__ == "__main__":
    # Run the async example
    asyncio.run(async_main())
    
    # Or run the sync example
    # main()