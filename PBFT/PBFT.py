import socket
#from sqlite3 import Timestamp
import threading
import json
import time
import random
from enum import Enum
import psutil
import tracemalloc


class PBFTState(Enum):
    NORMAL = "Normal"
    VIEW_CHANGE = "View Change"


class PBFTNode:
    def __init__(self, node_id, peers, port):
        self.node_id = node_id
        self.peers = peers  # {peer_id: (host, port)}
        self.port = port
        self.state = PBFTState.NORMAL
        self.view = 0
        self.primary_id = 1  # primary node for the current view
        self.log = []
        self.commit_index = -1
        self.received_pre_prepares = {}
        self.received_prepares = {}
        self.received_commits = {}
        self.received_view_change = {}
        self.commited = {}
        self.last_reply_timestamp = {}
        self.last_applied = -1
        self.sequence = 0
        self.f = len(peers) // 3
        self.timer = {}
        self.timeout = 5
        self.log_checkpoint = -1
        self.received_checkpoints = {}
        self.view_change_s = []

        self.client_replies = {}  # {request_id: {"replies": set(), "timestamp": float}}
        self.reply_timeout = 3000  # Timeout in seconds for collecting replies
        self.client_lock = threading.Lock()

        self.socket = None
        self.stop_event = threading.Event()

        self.log_evaluation_view = True

        self.evaluation_log_file = f"node_{self.node_id}_evaluation.log"

        _v = ""
        if self.log_evaluation_view: _v = ",VIEW"
        with open(self.evaluation_log_file, "w") as f:
            f.write(f"NodeID,Timestamp,Action,DataTime,CPU,MEMORY{_v}\n")

    def start(self):
        threading.Thread(target=self.run_server).start()
        threading.Thread(target=self.run_state_manager).start()
        threading.Thread(target=self.generate_iot_message).start()
        threading.Thread(target=self.apply_logs_to_state_machine, daemon=True).start()
        threading.Thread(target=self.monitor_checkpoints, daemon=True).start()
        threading.Thread(target=self.monitor_timers).start()
        threading.Thread(target=self.monitor_replies).start()

    def run_server(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("0.0.0.0", self.port))
        self.socket.listen(5)
        print(f"Node {self.node_id} listening on port {self.port}...")

        while not self.stop_event.is_set():
            client_socket, _ = self.socket.accept()
            threading.Thread(target=self.handle_connection, args=(client_socket,)).start()

    def log_evaluation(self, action, datatime,view=-1):
        """Log evaluation data for analysis."""
        timestamp = time.time()

        """Log CPU and memory usage as actual values."""
        # Get the CPU times (user + system) for the current process
        process = psutil.Process()
        cpu_times = process.cpu_times()
        cpu_usage = cpu_times.user + cpu_times.system  # Total CPU time in seconds

        # Get memory usage for the current process in bytes
        memory_info = process.memory_info()
        memory_usage = memory_info.rss  # Resident Set Size (RSS) in bytes
        #memory_usage = memory_info.vms # The total virtual memory used by the process.

        #current, peak = tracemalloc.get_traced_memory()

        #memory_usage = current
        _v = ""
        if self.log_evaluation_view: _v=f",{view}"
        with open(self.evaluation_log_file, "a") as f:
            f.write(f"{self.node_id},{timestamp},{action},{datatime},{cpu_usage},{memory_usage}{_v}\n")
            f.flush()

    def handle_message(self, message):

        

        message_type = message["type"]

        print(f"receive message, message {message}")

        if message_type == "PRE_PREPARE":
            self.handle_pre_prepare(message)
        elif message_type == "PREPARE":
            self.handle_prepare(message)
        elif message_type == "COMMIT":
            self.handle_commit(message)
        elif message_type == "REQUEST":
            self.handle_request(message)
        elif message_type == "CHECKPOINT":
            self.handle_checkpoint(message)
        elif message_type == "LOG_RECOVERY":
            self.handle_log_recovery(message)
        elif message_type == "LOG_RESPONSE":
            self.handle_log_response(message)
        elif message_type == "VIEW_CHANGE":
            self.handle_view_change(message)
        elif message_type == "NEW_VIEW":
            self.handle_new_view(message)
        elif message_type == "REPLY":
            self.handle_reply(message)

        self.log_evaluation("RECEIVE_MESSAGE", "")

    # Checkpoint mechanism
    def create_checkpoint(self):
        stable_sequence = self.last_applied
        checkpoint_message = {
            "type": "CHECKPOINT",
            "node_id": self.node_id,
            "sequence": stable_sequence,
        }
        self.broadcast_message(checkpoint_message)
        if stable_sequence not in self.received_checkpoints:
            self.received_checkpoints[stable_sequence] = set()
        self.received_checkpoints[stable_sequence].add(node_id)

    def handle_checkpoint(self, message):
        sequence = message["sequence"]
        node_id = message["node_id"]
        if sequence > self.log_checkpoint:
            if sequence not in self.received_checkpoints:
                self.received_checkpoints[sequence] = set()
            self.received_checkpoints[sequence].add(node_id)

            if len(self.received_checkpoints[sequence]) > self.f * 2:
                self.log_checkpoint = sequence  # Mark as stable

    def monitor_checkpoints(self):
        while not self.stop_event.is_set():
            if self.last_applied > 0 and self.last_applied % 10 == 0:
                self.create_checkpoint()
            time.sleep(1)

    def recover_missing_logs(self):
        if self.log_checkpoint > self.last_applied:
            message = {
                "type": "LOG_RECOVERY",
                "sequence": self.last_applied + 1,
                "node_id": self.node_id,
            }
            self.broadcast_message(message)

    def handle_log_recovery(self, message):
        if self.node_id == self.primary_id:
            start_sequence = message["sequence"]
            data_to_send = []
            end_sequence = min(start_sequence + 5, self.log_checkpoint + 1)
            for seq in range(start_sequence, end_sequence):
                for (v,s) in self.commited:
                    if seq == s: 
                        data_to_send.append({"sequence": seq, "view": v, "data":self.commited[(v, seq)]})
                        break
            response_message = {
                "type": "LOG_RESPONSE",
                "data": data_to_send,
                "node_id": self.node_id,
            }
            self.send_message(message["node_id"], response_message)

    def handle_log_response(self, message):
        for data in message["data"]:
            view = data["view"]
            sequence = data["sequence"]
            self.commited[(view, sequence)] = data["data"]
            #self.last_applied = sequence

    def handle_connection(self, client_socket):
        with client_socket:
            data = client_socket.recv(1048576)
            if data:
                message = json.loads(data.decode("utf-8"))
                self.handle_message(message)

    def generate_iot_message(self):
        while not self.stop_event.is_set():

            data = {
                "node_id": self.node_id,
                "time": round(time.time(),2),
                "temperature": round(random.uniform(15, 30),2),
                "humidity": round(random.uniform(30, 60),2),
                "air_quality": round(random.uniform(50, 100),2),
            }

            print(f"generated message, primary {self.primary_id}")

            if json.dumps(data) not in self.timer:
                self.timer[json.dumps(data)] = time.time() + self.timeout

            if self.node_id == self.primary_id:
                
                self.broadcast_pre_prepare(data)

            elif self.primary_id is not None:
                # Send data to the leader
                self.send_message(self.primary_id, {
                    "type": "REQUEST",
                    "data": data
                })
                print(f"generated message sent to {self.primary_id}")

                

            self.client_replies[data["time"]] = {"replies":set(), "send_time": time.time(), "data":data}
            time.sleep(5)

    def broadcast_pre_prepare(self, data):

        print(f"broadcast_pre_prepare {data}")

        if self.state == PBFTState.NORMAL and self.node_id == self.primary_id:

            broadcast_sequence = self.sequence

            has_data_in_pre_prepare = False

            for k, v in self.received_pre_prepares.items():
                if data == v:
                    broadcast_sequence = k[1]
                    has_data_in_pre_prepare = True


            message = {
                "type": "PRE_PREPARE",
                "view": self.view,
                "data": data,
                "node_id": self.node_id,
                "sequence": broadcast_sequence,
            }

            self.log_evaluation("BROADCAST", data["time"])

            if not has_data_in_pre_prepare:
                self.received_pre_prepares[(self.view, self.sequence)] = data
                self.sequence += 1

            if json.dumps(data) not in self.timer:
                self.timer[json.dumps(data)] = time.time() + self.timeout
            self.log.append(message)

            self.broadcast_message(message)

    def handle_pre_prepare(self, message):

        print(f"handle_pre_prepare ")

        view = message["view"]
        sequence = message["sequence"]
        data = message["data"]
        client_id = data["node_id"]
        timestamp = data["time"]

        if client_id in self.last_reply_timestamp and timestamp <= self.last_reply_timestamp[client_id]:
            print(f"Duplicate or outdated request from client {client_id}. Ignoring.")           
            return

        if (view,sequence) in self.received_pre_prepares and self.received_pre_prepares[(view,sequence)] != message["data"]:
            return
        if view == self.view and self.state == PBFTState.NORMAL:
            self.log.append(message)
            self.received_pre_prepares[(view,sequence)] = data
            if json.dumps(data) not in self.timer:
                self.timer[json.dumps(data)] = time.time() + self.timeout

            prepare_message = {
                "type": "PREPARE",
                "view": self.view,
                "data": data,
                "node_id": self.node_id,
                "sequence": sequence,
            }
            self.broadcast_message(prepare_message)
            self.log.append(prepare_message)
            key = (view, sequence, json.dumps(data))
            if key not in self.received_prepares:
                self.received_prepares[key] = set()
            self.received_prepares[key].add(message["node_id"])

    def handle_prepare(self, message):
        view = message["view"]
        sequence = message["sequence"]
        data = message["data"]

        print(f"handle_prepare ")

        if view == self.view and self.state == PBFTState.NORMAL:
            self.log.append(message)

            data_json = json.dumps(data)

            key = (view, sequence, data_json)
            if key not in self.received_prepares:
                self.received_prepares[key] = set()
            self.received_prepares[key].add(message["node_id"])

            print(f"count prepare for view {view}, sequence {sequence}",len(self.received_prepares[key]))
            #print("received_pre_prepares", self.received_pre_prepares)
            

            pre_prepares_keys = [k for k, v in self.received_pre_prepares.items() if v == data]

            print("pre_prepares_keys",pre_prepares_keys)

            if (view,sequence) in pre_prepares_keys and len(self.received_prepares[key]) >= self.f * 2:
                if (key not in self.received_commits) or (message["node_id"] not in self.received_commits[key]):
                    commit_message = {
                        "type": "COMMIT",
                        "view": self.view,
                        "data": data,
                        "node_id": self.node_id,
                        "sequence": sequence,
                    }
                    self.broadcast_message(commit_message)
                    self.log.append(commit_message)

                    if key not in self.received_commits:
                        self.received_commits[key] = set()
                    self.received_commits[key].add(message["node_id"])

    def handle_commit(self, message):

        print(f"handle_commit {message}")

        view = message["view"]
        sequence = message["sequence"]
        data = message["data"]
        timestamp = data["time"]
        client_id = data["node_id"]

        if view == self.view and self.state == PBFTState.NORMAL and (self):
            self.log.append(message)

            data_json = json.dumps(data)
            key = (view, sequence, data_json)
            if key not in self.received_commits:
                self.received_commits[key] = set()
            self.received_commits[key].add(message["node_id"])

            if key in self.received_prepares and len(self.received_prepares[key]) >= self.f * 2 and len(self.received_commits[key]) > self.f * 2:
                if (view, sequence) not in self.commited:
                    self.commited[(view, sequence)] = data
                    if json.dumps(data) in self.timer:
                        del self.timer[json.dumps(data)]
                    print(f"Data committed: {message['data']}")
                    self.log_evaluation("COMMIT", timestamp,view)

    def apply_logs_to_state_machine(self):
        while True:
            keys = sorted(self.commited.keys())
            for view, sequence in keys:
                if sequence == self.last_applied + 1:
                    data = self.commited[(view, sequence)]
                    client_id = data["node_id"]
                    timestamp = data["time"]

                    # Write to the log file
                    with open(f"node_{self.node_id}_messages.log", "a") as file:
                        file.write(json.dumps(data) + "\n")
                        file.flush()

                    # Update last applied and reply timestamp
                    self.last_reply_timestamp[client_id] = timestamp
                    self.last_applied += 1
                    break

            # Sleep briefly to avoid busy-waiting
            time.sleep(0.1)

    def monitor_timers(self):
        """Monitor timers for timeout and initiate view change if necessary."""
        while not self.stop_event.is_set() and self.state == PBFTState.NORMAL:
        #while not self.stop_event.is_set():
            current_time = time.time()
            print(f"timer: {self.timer}")
            for data_json, timeout in self.timer.items():
                if current_time > timeout:
                    print(f"Timeout for {data_json}. Initiating view change.")
                    #del self.timer[(view, sequence)]
                    #self.start_view_change()
                    self.state = PBFTState.VIEW_CHANGE
                    break  # Break to avoid modifying the dict while iterating
            time.sleep(0.1)

    def handle_request(self, message):
        if self.node_id == self.primary_id:
            self.broadcast_pre_prepare(message["data"])
        else:
            data = message["data"]
            client_id = data["node_id"]
            timestamp = data["time"]

            # Check if the request has been committed or applied
            if any(
                entry["time"] == timestamp and entry["node_id"] == client_id
                for entry in self.commited.values()
            ):
                # If found, reply directly
                self.reply_to_client(data)
            else:
                # Relay the request to the primary

                self.send_message(self.primary_id, message)

            if json.dumps(data) not in self.timer:
                self.timer[json.dumps(data)] = time.time() + self.timeout

    def reply_to_client(self, data, success=True):
        """Send a reply to the client."""
        message = {
            "type": "REPLY",
            "success": success,
            "node_id": self.node_id,
            "time": data["time"]
        }

        if data["node_id"] == self.node_id:
            self.handle_reply(message)
        else:
            self.send_message(data["node_id"], message)

    def handle_reply(self, message):
        """Handle replies from replicas."""
        data_time = message["time"]
        node_id = message["node_id"]

        # Add the reply to the set
        if data_time in self.client_replies:
            self.client_replies[data_time]["replies"].add(node_id)

            # If sufficient replies are collected, finalize the request
            if len(self.client_replies[data_time]["replies"]) >= self.f + 1:
                del self.client_replies[data_time]

    def monitor_replies(self):
        """Monitor pending client replies and resend if necessary."""
        while not self.stop_event.is_set():
            current_time = time.time()

            for request_id, info in list(self.client_replies.items()):
                if current_time - info["send_time"] > self.reply_timeout:
                    print(f"Timeout for request {request_id}. Resending to primary.")

                    request_message = {
                            "type": "REQUEST",
                            "data": info["data"]
                        }
                    self.broadcast_message(request_message)
                    self.handle_request(request_message)

                    self.client_replies[info["data"]["time"]] = {"replies":set(), "send_time": time.time(), "data":info["data"]}


            time.sleep(0.1)

    def broadcast_message(self, message):
        for peer_id in self.peers:
            self.send_message(peer_id, message)

    def send_message(self, peer_id, message):
        host, port = self.peers[peer_id]
        self.log_evaluation("SEND_MESSAGE", "")
        print(f"send message, peer id {peer_id}, message {message}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((host, port))
                s.sendall(json.dumps(message).encode("utf-8"))
            except:
                print(f"Node {self.node_id} could not connect to Node {peer_id}.")

    def run_state_manager(self):
        while not self.stop_event.is_set():
            print(f"state {self.state}")
            if self.state == PBFTState.NORMAL:
                self.run_normal()
            elif self.state == PBFTState.VIEW_CHANGE:
                self.run_view_change()


    def run_normal(self):
        while self.state == PBFTState.NORMAL:
            self.recover_missing_logs()
            time.sleep(1)
            print(f"state {self.state}")

    def run_view_change(self):
        while self.state == PBFTState.VIEW_CHANGE:
            self.start_view_change()
        #while self.state == PBFTState.VIEW_CHANGE:
            #time.sleep(1)
            time.sleep(self.timeout)
            print(f"state {self.state}")


    def update_primary(self):
        """Update the primary node based on the current view."""
        self.primary_id = self.view % (len(self.peers)+1) + 1

    def start_view_change(self):
        """Initiate a view change when the primary fails."""
        self.view += 1  # Increment the view
        self.update_primary()
        self.timer.clear() 
        #self.state = PBFTState.VIEW_CHANGE
        print(f"View change initiated. New view: {self.view}, New primary: {self.primary_id}")

        # Notify all peers about the view change
        message = {
            "type": "VIEW_CHANGE",
            "view": self.view,
            "sequence": self.last_applied,
            "node_id": self.node_id,
        }
        self.broadcast_message(message)

    def handle_view_change(self, message):
        """Handle a view change message."""
        view = message["view"]
        sequence = message["sequence"]
        if view >= self.view and view % (len(self.peers)+1)+1 == self.node_id:
            if view not in self.received_view_change:
                self.received_view_change[view] = set()
            self.received_view_change[view].add(message["node_id"])
            self.view_change_s.append(sequence)

            if len(self.received_view_change[view]) >= self.f*2:
                self.view = view
                self.update_primary()
                self.state = PBFTState.NORMAL
                message = {
                    "type": "NEW_VIEW",
                    "view": self.view,
                }
                self.broadcast_message(message)
                print(f"View change handled. Updated view: {self.view}, New primary: {self.primary_id}")

                min_s = min(self.view_change_s)
                pre_prepare_seq = [s for (v, s) in self.received_pre_prepares.keys()]
                max_s = max(pre_prepare_seq) + 1
                for seq in range(min_s, max_s):
                    for keys in self.received_pre_prepares:
                        if seq == keys[1]:                    
                            data = self.received_pre_prepares[keys]
                            self.sequence = seq
                            self.broadcast_pre_prepare(data)
                            break
                self.view_change_s = []
                self.sequence = max_s
                self.timer.clear()

    def handle_new_view(self, message):
        view = message["view"]
        self.view = view
        self.view_change_s = []
        self.timer.clear()
        self.state = PBFTState.NORMAL
        self.update_primary()

    def stop(self):
        self.stop_event.set()
        if self.socket:
            self.socket.close()


if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    total_nodes_num = int(sys.argv[4])

    with open(sys.argv[3], 'r') as f:
        peers = json.load(f)

    converted_peers = {key: peers[str(key)] for key in range(1, total_nodes_num + 1)}
    del converted_peers[node_id]

    node = PBFTNode(node_id, converted_peers, port)
    try:
        node.start()
    except KeyboardInterrupt:
        node.stop()
