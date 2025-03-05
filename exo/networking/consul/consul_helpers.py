import json
import asyncio
import aiohttp
from typing import Dict, Any, Tuple, List, Optional
from exo.helpers import DEBUG_DISCOVERY
from exo.topology.device_capabilities import DeviceCapabilities, DeviceFlops
from datetime import datetime, timezone

class Device:
#    @staticmethod
#    def parse_datetime(date_string: Optional[str]) -> Optional[datetime]:
#        if not date_string:
#            return None
#        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    def __str__(self):
        # Get all attributes and their values
        attributes = ", ".join(f"{key}: {value}" for key, value in self.__dict__.items())
        return f"Device({attributes})"


async def get_consul_devices(consul_url: str) -> Dict[str, Device] :
    """
    get_consul_devices queries Consul service to get registred peers
    """
    async with aiohttp.ClientSession() as session:
        url = f"{consul_url}/v1/catalog/service/exo"
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            #print(f"*consul:disco* json {data}")
            devices = {}
            for instance in data:
                # create new device
                device = Device()
                #device.ID = instance.get("ID",""),
                device.name = instance.get("ID",""),
                device.addresses = [instance.get("Address","")]
                device.lastseen = instance.get("CreateIndex","")
                
                meta = instance["ServiceMeta"]
                device.nodeid =  meta.get("node_id","")
                device.nodeport = meta.get("node_port","")

                device.capabilities=  DeviceCapabilities(
                    model=meta.get("device_capability_model", ""),
                    chip=meta.get("device_capability_chip", ""),
                    memory=meta.get("device_capability_memory", 0),
                    flops=DeviceFlops(
                        fp16=float(meta.get("device_capability_flops_fp16", 0)),
                        fp32=float(meta.get("device_capability_flops_fp32", 0)),
                        int8=float(meta.get("device_capability_flops_int8", 0))
                    )
                )
                print("rrrrr")
                print(device)

                devices[device.name] = device
                #print(f"*consul update* New device:{device}")
            return devices




async def update_consul_attributes(node_id: str, node_port: int, device_capabilities: DeviceCapabilities, consul_url: str):
    """
    update_consul_attributes registers Consul or updates its metadata 

    """
    print(f"*consul update* Capabilities: {node_id} {node_port} {device_capabilities}")
    metadata = {
      "node_id": node_id,
      "node_port": str(node_port),
      "device_capability_chip": device_capabilities.chip,
      "device_capability_model": device_capabilities.model,
      "device_capability_memory": str(device_capabilities.memory),
      "device_capability_flops_fp32": str(device_capabilities.flops.fp32),
      "device_capability_flops_fp16": str(device_capabilities.flops.fp16),
      "device_capability_flops_int8": str(device_capabilities.flops.int8)
    }
    
    headers = {
        "Content-type": "application/json"
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        url = f"{consul_url}/v1/agent/service/register"
        service_definition = {
            "ID": node_id,
            "Name": "exo",
            "Meta": metadata,
        }
        raw_json_request = json.dumps(service_definition, indent=2)
        #print(f"*disco* Json to Consul {raw_json_request}")

        async with session.put(url, json=service_definition) as response:
            if response.status == 200:
                print(f"*consul update* Registration OK current node {node_id}")
            else:
                p = await response.content.read()
                #print(f"*disco* Failed to register service for node {node_id}: HTTP {response.status} {p}")





#async def get_consul_attributes(device_id: str, consul_url: str) -> Tuple[str, int, DeviceCapabilities]:
#    async with aiohttp.ClientSession() as session:
#        url = f"{consul_url}/v1/kv/exo/node/{device_id}?raw"
#        async with session.get(url) as response:
#            if response.status == 200:
#                data = await response.json()
#                node_id = data.get("node_id", "")
#                node_port = int(data.get("node_port", 0))
#                device_capabilities = DeviceCapabilities(
#                    model=data.get("device_capability_model", ""),
#                    chip=data.get("device_capability_chip", ""),
#                    memory=int(data.get("device_capability_memory", 0)),
#                    flops=DeviceFlops(
#                        fp16=float(data.get("device_capability_flops_fp16", 0)),
#                        fp32=float(data.get("device_capability_flops_fp32", 0)),
#                        int8=float(data.get("device_capability_flops_int8", 0))
#                    )
#                )
#                return node_id, node_port, device_capabilities
#            else:
#                print(f"*consul disco* Failed to fetch attributes for device {device_id}: {response.status}")
#                return "", 0, DeviceCapabilities(model="", chip="", memory=0, flops=DeviceFlops(fp16=0, fp32=0, int8=0))
#