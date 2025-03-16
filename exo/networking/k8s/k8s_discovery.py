import asyncio
import json
import time
import traceback
from typing import List, Dict, Callable, Tuple
from exo.networking.discovery import Discovery
from exo.networking.peer_handle import PeerHandle
from exo.topology.device_capabilities import DeviceCapabilities, device_capabilities, UNKNOWN_DEVICE_CAPABILITIES
from exo.helpers import DEBUG, DEBUG_DISCOVERY





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
        if DEBUG_DISCOVERY >= 2: print(f"*k8s start*")
        #self.device_capabilities = await device_capabilities()
        #self.discovery_task = asyncio.create_task(self.task_discover_peers())
        #self.cleanup_task = asyncio.create_task(self.task_cleanup_peers())
        #self.update_task = asyncio.create_task(self.task_update_device_posture_attributes())

        # Listen to other peers
        self.listen_task = asyncio.create_task(self.task_listen_for_peers())

    async def stop(self):
        if self.broadcast_task: self.broadcast_task.cancel()
        if self.listen_task: self.listen_task.cancel()
        if self.cleanup_task: self.cleanup_task.cancel()
        if self.broadcast_task or self.listen_task or self.cleanup_task:
            await asyncio.gather(self.broadcast_task, self.listen_task, self.cleanup_task, return_exceptions=True)


    async def task_update_device_posture_attributes(self):
        """
        task_update_device_posture_attributes periodically send or updates instance's registration and attributes
        """
        while True:
            try:
                await self.update_device_posture_attributes()
                #print("*consule update* Updated device posture attributes")
            except Exception as e:
                print(f"*consul update* Error updating device posture attributes: {e}")
                print(traceback.format_exc())
            finally:
                await asyncio.sleep(self.update_interval)


    async def task_listen_for_peers(self):
        """
        task_listen_for_peers Opens and process incoming messages from other peers       
        """
        if DEBUG_DISCOVERY >= 2: print(f"Started listen task on port {self.listen_port}")
        server = await asyncio.start_server(self.on_listen_message, '0.0.0.0', self.listen_port)
        if DEBUG_DISCOVERY >= 2: print(f"Server:{server}")
        addr = server.sockets[0].getsockname()
        if DEBUG_DISCOVERY >= 2: print(f"Started listen task on {addr}, port {self.listen_port}")
        async with server:
            await server.serve_forever()







    async def update_device_posture_attributes(self):
        await update_consul_attributes(self.node_id, self.node_port, self.device_capabilities, self.consul_url)



    def get_peers_ips_from_k8s_api(exo_namespace, exo_service_name):
        """
        get_peers_ips_from_k8s_api queries Kubernetes API to get endpoints        
        """
        # Load the in-cluster config
        config.load_incluster_config()

        # Create an instance of the API class
        v1 = client.CoreV1Api()

        try:
            # Get the endpoints of the service
            endpoints = v1.read_namespaced_endpoints(name=exo_service_name, namespace=exo_namespace)

            # Extract the IPs from the endpoints
            endpoint_ips = []
            if endpoints.subsets:
                for subset in endpoints.subsets:
                    if subset.addresses:
                        for address in subset.addresses:
                            endpoint_ips.append(address.ip)

            return endpoint_ips

        except client.ApiException as e:
           print(f"Exception when calling CoreV1Api->read_namespaced_endpoints: {e}\n")
           return []





    async def process_discovery(self,addr,message):
        if DEBUG_DISCOVERY >= 2: print(f"Process disco message {message}")

        peer_id=""
        if message["type"] == "discovery": 
           if DEBUG_DISCOVERY >= 2: print(f"message node_id: {message['node_id']}")
           if message["node_id"] != self.node_id:
              peer_id = message["node_id"]
           else: 
              if DEBUG_DISCOVERY >= 2: print(f"same node_id as the current instance, skipping this disco message")
              return
        else:
            if DEBUG_DISCOVERY >= 2: print(f"message not type discovery, skip")
            return
        if DEBUG_DISCOVERY >= 2: print(f"Here")

        # Skip if peer_id is not in allowed list
        if self.allowed_node_ids and peer_id not in self.allowed_node_ids:
            if DEBUG_DISCOVERY >= 2: print(f"Ignoring peer {peer_id} as it's not in the allowed node IDs list")
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
                print(f"Ignoring peer {peer_id} at {peer_host}:{peer_port} with priority {peer_prio} because we already know about a peer with higher or equal priority: {existing_peer_prio}")
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



    async def on_listen_message(self, reader, writer):
        """
        task_listen_for_peers Process TCP messages        
        """
        if DEBUG_DISCOVERY >= 2: print(f"Streams Reader:{reader}")
        if DEBUG_DISCOVERY >= 2: print(f"Streams Writer{writer}")
        
        try: 
            sock = writer.get_extra_info('socket')
            if DEBUG_DISCOVERY >= 2: print(f"Socket: {sock}")

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
                        if DEBUG_DISCOVERY >= 2: print(f"Error decoding JSON data from {addr}: {e}")
                        continue
                    if DEBUG_DISCOVERY >= 2: print(f"received from peer {addr}: {message}")
                    await self.process_discovery(addr,message)


                    # Process decoded_data as needed
                    # HERE PROCESS JSON

                except UnicodeDecodeError:
                    print("Received data is not valid UTF-8")

            print(f"B Received data: {decoded_data}")
        except Exception as e:
           print(Exception,e)
           print(traceback.format_exc())

           return []





    async def discover_peers(self, wait_for_peers: int = 0) -> List[PeerHandle]:
        if wait_for_peers > 0:
            while len(self.known_peers) < wait_for_peers:
                if DEBUG_DISCOVERY >= 2: print(f"Current peers: {len(self.known_peers)}/{wait_for_peers}. Waiting for more peers...")
                await asyncio.sleep(0.1)
        return [peer_handle for peer_handle, _, _, _ in self.known_peers.values()]
