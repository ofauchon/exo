import asyncio
import json
import time
import traceback
import socket
from typing import List, Dict, Callable, Tuple
from exo.networking.discovery import Discovery
from exo.networking.peer_handle import PeerHandle
from exo.topology.device_capabilities import DeviceCapabilities, device_capabilities, UNKNOWN_DEVICE_CAPABILITIES
from exo.helpers import DEBUG, DEBUG_DISCOVERY, get_all_ip_addresses_and_interfaces, get_interface_priority_and_type
from kubernetes import client, config

def log_d(*args, **kwargs):
    if DEBUG_DISCOVERY == 2: print("k8s_disco DBG:", *args, **kwargs)


def log_i(*args, **kwargs):
    if DEBUG_DISCOVERY == 1: print("k8s_disco NFO:", *args, **kwargs)
    
def log_e(*args, **kwargs):
    print("k8s_disco ERR:", *args, **kwargs)



class K8SDiscovery(Discovery):
    def __init__(
        self,
        node_id: str,
        node_port: int,
        listen_port: int,
        broadcast_port: int,
        create_peer_handle: Callable[[str, str, str, DeviceCapabilities], PeerHandle],
        broadcast_interval: int = 2.5,
        discovery_timeout: int = 30,
        device_capabilities: DeviceCapabilities = UNKNOWN_DEVICE_CAPABILITIES,
        allowed_node_ids: List[str] = None,
        allowed_interface_types: List[str] = None,


    ):
        self.node_id = node_id
        self.node_port = node_port
        self.listen_port = listen_port
        self.broadcast_port = broadcast_port
        self.create_peer_handle = create_peer_handle
        self.broadcast_interval = broadcast_interval
        self.discovery_timeout = discovery_timeout
        self.device_capabilities = device_capabilities
        self.allowed_node_ids = allowed_node_ids
        self.allowed_interface_types = allowed_interface_types
        self.known_peers: Dict[str, Tuple[PeerHandle, float, float, int]] = {}
        self.broadcast_task = None
        self.listen_task = None
        self.cleanup_task = None

    async def start(self):
        log_i("Start k8s discovery plugin")
        # Send updates to peers
        self.notify_peers_for_updates_task = asyncio.create_task(self.task_notify_peers_for_updates())
        # Listen to other peers
        self.listen_for_peers_task = asyncio.create_task(self.task_listen_for_peers())


    async def stop(self):
        if self.broadcast_task: self.broadcast_task.cancel()
        if self.listen_task: self.listen_task.cancel()
        if self.cleanup_task: self.cleanup_task.cancel()
        if self.broadcast_task or self.listen_task or self.cleanup_task:
            await asyncio.gather(self.broadcast_task, self.listen_task, self.cleanup_task, return_exceptions=True)


    # 
    # Task : Connects all k8s exo hosts and send them capabilities update
    # 

    async def task_notify_peers_for_updates(self):
        """
        task_notify_peers_for_updates periodically send or updates instance's registration and attributes
        """
        while True:
            try:
                await self.notify_peers_for_updates()
            except Exception as e:
                log_e(f"Error updating device posture attributes: {e}")
                print(traceback.format_exc())
            finally:
                await asyncio.sleep(self.update_interval)



    async def notify_peers_for_updates(self):

      for addr, interface_name in get_all_ip_addresses_and_interfaces():
        interface_priority, interface_type = await get_interface_priority_and_type(interface_name)
        message = json.dumps({
          "type": "discovery",
          "node_id": self.node_id,
          "grpc_port": self.node_port,
          "device_capabilities": self.device_capabilities.to_dict(),
          "priority": interface_priority,
          "interface_name": interface_name,
          "interface_type": interface_type,
        })
        log_d(f"Broadcast message {message}")

        peers = self.get_peers_ips_from_k8s_api("exo", "exo")
        for peer in peers:
            log_d(f"Send message to {peer}")
            self.send_message_to_server(peer,self.listen_port, message)



    def get_peers_ips_from_k8s_api(self,exo_namespace, exo_service_name):
        """
        get_peers_ips_from_k8s_api queries Kubernetes API to get endpoints        
        """
        config.load_incluster_config()
        v1 = client.CoreV1Api()

        try:
            endpoints = v1.read_namespaced_endpoints(name=exo_service_name, namespace=exo_namespace)
            endpoint_ips = []
            if endpoints.subsets:
                for subset in endpoints.subsets:
                    if subset.addresses:
                        for address in subset.addresses:
                            endpoint_ips.append(address.ip)
            return endpoint_ips

        except client.ApiException as e:
           log_e(f"Exception when calling CoreV1Api->read_namespaced_endpoints: {e}\n")
           return []



    def get_peers_ips_from_k8s_api2(self,exo_namespace, exo_service_name):
        """
        get_peers_ips_from_k8s_api2 Is a fake service
        """
        return ["127.0.0.2","127.0.0.3"]


    def send_message_to_server(self, host, port, message):
        # Create a TCP/IP socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to the server
            log_d(f"Connecting to {host}:{port}")
            sock.connect((host, port))

            # Send the message
            sock.sendall(message.encode('utf-8'))

    # ######################################
    # Task for listening peers updates below
    # ######################################

    async def task_listen_for_peers(self):
        """
        task_listen_for_peers Opens and process incoming messages from other peers       
        """
        server = await asyncio.start_server(self.on_listen_message, '0.0.0.0', self.listen_port)
        addr = server.sockets[0].getsockname()
        log_i(f"Listening on {addr}, port {self.listen_port}")
        async with server:
            await server.serve_forever()

    async def on_listen_message(self, reader, writer):
        """
        on_listen_message Process TCP messages        
        """
        log_d(f"Streams Reader:{reader}")
        log_d(f"Streams Writer{writer}")
        
        try: 
            sock = writer.get_extra_info('socket')
            log_d(f"Socket: {sock}")

            peer=sock.getpeername()

            addr=peer[0]
            if DEBUG_DISCOVERY >= 2: print(f"Connected to {addr}")


            decoded_data=""
            while data := await reader.read(1000):
                try:
                    decoded_data = data.decode("utf-8")
                    print(f"Received data: {decoded_data}")

                    # Check if the decoded data starts with a valid JSON character
                    if not (decoded_data.strip() and decoded_data.strip()[0] in "{["):
                        if DEBUG_DISCOVERY >= 2: print(f"Received invalid JSON data from {addr}: {decoded_data[:100]}")
                        continue
                    # Try to decode JSON
                    try:
                        decoder = json.JSONDecoder(strict=False)
                        message = decoder.decode(decoded_data)
                    except json.JSONDecodeError as e:
                        log_e(f"Error decoding JSON data from {addr}: {e}")
                        continue
                    log_i(f"received from peer {addr}: {message}")
                    await self.handle_received_message(addr,message)

                    # Process decoded_data as needed
                    # HERE PROCESS JSON

                except UnicodeDecodeError:
                    print("Received data is not valid UTF-8")

            log_d(f"B Received data: {decoded_data}")
        except Exception as e:
           print(Exception,e)
           print(traceback.format_exc())

           return []


    async def handle_received_message(self,addr,message):
        """
        handle_received_message parses and processes messages from other peers
        """
        if DEBUG_DISCOVERY >= 2: print(f"Process disco message {message}")

        peer_id=""
        if message["type"] == "discovery": 
           log_d(f"message node_id: {message['node_id']}")
           if message["node_id"] != self.node_id:
              peer_id = message["node_id"]
           else: 
              log_i(f"same node_id as the current instance, skipping this disco message")
              return
        else:
            log_i(f"message not type discovery, skip")
            return
        if DEBUG_DISCOVERY >= 2: print(f"Here")

        # Skip if peer_id is not in allowed list
        if self.allowed_node_ids and peer_id not in self.allowed_node_ids:
            log_i(f"Ignoring peer {peer_id} as it's not in the allowed node IDs list")
            return
        # Create new peer object
        peer_host = addr
        peer_port = message["grpc_port"]
        peer_prio = message["priority"]
        peer_interface_name = message["interface_name"]
        peer_interface_type = message["interface_type"]
        if DEBUG_DISCOVERY >= 2: print(f"Here2")
        # Skip if interface type is not in allowed list
        if self.allowed_interface_types and peer_interface_type not in self.allowed_interface_types:
            if DEBUG_DISCOVERY >= 2: print(f"Ignoring peer {peer_id} as its interface type {peer_interface_type} is not in the allowed interface types list")
            return
        if DEBUG_DISCOVERY >= 2: print(f"Here3 peer_id {peer_id}")

        device_capabilities = DeviceCapabilities(**message["device_capabilities"])
        if peer_id not in self.known_peers or self.known_peers[peer_id][0].addr() != f"{peer_host}:{peer_port}":
          if peer_id in self.known_peers:
            existing_peer_prio = self.known_peers[peer_id][3]
            if existing_peer_prio >= peer_prio:
              if DEBUG >= 1:
                log_i(f"Ignoring peer {peer_id} at {peer_host}:{peer_port} with priority {peer_prio} because we already know about a peer with higher or equal priority: {existing_peer_prio}")
              return
          if DEBUG_DISCOVERY >= 2: print(f"NEW  peer {peer_host}:{peer_port}")
          new_peer_handle = self.create_peer_handle(peer_id, f"{peer_host}:{peer_port}", f"{peer_interface_type} ({peer_interface_name})", device_capabilities)
          #if not await new_peer_handle.health_check():
          #  if DEBUG >= 1: print(f"Peer {peer_id} at {peer_host}:{peer_port} is not healthy. Skipping.")
          #  return
          if DEBUG >= 1: print(f"Adding {peer_id=} at {peer_host}:{peer_port}. Replace existing peer_id: {peer_id in self.known_peers}")
          self.known_peers[peer_id] = (new_peer_handle, time.time(), time.time(), peer_prio)
        else:
          if not await self.known_peers[peer_id][0].health_check():
            if DEBUG >= 1: print(f"Peer {peer_id} at {peer_host}:{peer_port} is not healthy. Removing.")
            if peer_id in self.known_peers: del self.known_peers[peer_id]
            return
          if peer_id in self.known_peers: self.known_peers[peer_id] = (self.known_peers[peer_id][0], self.known_peers[peer_id][1], time.time(), peer_prio)






    async def discover_peers(self, wait_for_peers: int = 0) -> List[PeerHandle]:
        if wait_for_peers > 0:
            while len(self.known_peers) < wait_for_peers:
                if DEBUG_DISCOVERY >= 2: print(f"Current peers: {len(self.known_peers)}/{wait_for_peers}. Waiting for more peers...")
                await asyncio.sleep(0.1)
        return [peer_handle for peer_handle, _, _, _ in self.known_peers.values()]
