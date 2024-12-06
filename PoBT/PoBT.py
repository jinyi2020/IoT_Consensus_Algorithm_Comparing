import socket
import threading
import random
import time
import json
from enum import Enum
import psutil


class State(Enum):
    DEVICE = "Device"
    PEER = "Peer"


class PoBTNode:
    def __init__(self, node_id, peers, port):
        self.node_id = node_id
        self.peers = peers  # Dictionary {peer_id: (host, port)}
        self.port = port
        self.state = State.PEER
        self.trades = []  # Trade pool
        self.session_nodes = []  # Nodes participating in the current session
        self.ledger = []  # Blockchain ledger
        self.socket = None
        self.orderer_id = max(max(peers.keys()), self.node_id)  # Choose the highest node ID as the orderer
        self.stop_event = threading.Event()
        self.active_blocking = False
        self.session_timeout = 15  # Maximum duration for trade collection
        self.consensus_timeout = 5  # Timeout for waiting for consensus responses
        self.session_start_time = time.time()  # Track when the session starts
        self.last_commit = -1
        self.last_received = -1
        self.last_applied = -1
        self.consensus_response = set()
        self.evaluation_log_file = f"node_{self.node_id}_evaluation.log"
        with open(self.evaluation_log_file, "w") as f:
            f.write("NodeID,Timestamp,Action,DataTime,CPU,MEMORY\n")

        self.block_len_max = 50

    def log_evaluation(self, action, datatime):
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
        with open(self.evaluation_log_file, "a") as f:
            f.write(f"{self.node_id},{timestamp},{action},{datatime},{cpu_usage},{memory_usage}\n")
            f.flush()

    def start(self):
        """Start the node."""
        threading.Thread(target=self.run_server).start()
        threading.Thread(target=self.run_consensus_manager).start()
        threading.Thread(target=self.generate_iot_message, daemon=True).start()
        threading.Thread(target=self.apply_logs_to_state_machine, daemon=True).start()

    def run_server(self):
        """Run the server to handle incoming messages."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("0.0.0.0", self.port))
        self.socket.listen(5)
        print(f"Node {self.node_id} listening on port {self.port}...")

        while not self.stop_event.is_set():
            client_socket, _ = self.socket.accept()
            threading.Thread(target=self.handle_connection, args=(client_socket,)).start()

    def handle_connection(self, client_socket):
        with client_socket:
            data = client_socket.recv(1048576)
            if data:
                message = json.loads(data.decode("utf-8"))
                self.handle_message(message)

    def handle_message(self, message):
        print(f"REVEIVE MESSAGE: {message}")
        self.log_evaluation("RECEIVE_MESSAGE", "")
        """Handle incoming messages."""
        if message["type"] == "trade":
            self.handle_trade(message)
        elif message["type"] == "consensus_request":
            self.handle_consensus_request(message)
        elif message["type"] == "consensus_response":
            self.handle_consensus_response(message)
        elif message["type"] == "commit":
            self.handle_commit(message)

    def generate_iot_message(self):
        """Generate and send IoT data periodically."""
        while not self.stop_event.is_set():
            # Generate IoT data
            data = {
                "node_id": self.node_id,
                "time": round(time.time(), 2),
                "temperature": round(random.uniform(15, 30), 2),
                "humidity": round(random.uniform(30, 60), 2),
                "air_quality": round(random.uniform(50, 100), 2),
            }
            print(f"Node {self.node_id} generated data: {data}")

            # Forward to orderer
            if self.node_id == self.orderer_id:
                self.trades.append(data)  # Add to trade pool if this node is the orderer
            else:
                self.send_message(self.orderer_id, {"type": "trade", "data": data})

            time.sleep(5)  # Generate data every 5 seconds

    def handle_trade(self, message):
        """Handle a new trade."""
        print(f"Node {self.node_id} received trade")
        self.trades.append(message["data"])

    def run_consensus_manager(self):
        """Manage the consensus process."""
        while not self.stop_event.is_set():
            time.sleep(0.005)  # Check every second

            # Start a new session if not blocked and trades are available
            if self.node_id == self.orderer_id and self.trades and not self.active_blocking:
                current_time = time.time()

                # Check if session timeout has been reached or no session is active
                if len(self.trades) > self.block_len_max or current_time - self.session_start_time > self.session_timeout:
                    print(f"Session timeout reached and no active session. Starting a new session.")
                    self.start_consensus_session()

    def start_consensus_session(self):

        """Start a new consensus session."""
        self.active_blocking = True
        #self.session_start_time = time.time()  # Record the start time of the session
        self.last_received = min(len(self.trades), self.block_len_max) - 1
        self.consensus_response = set()

        for data in self.trades:
            data_timestamp = data["time"]
            self.log_evaluation("BROADCAST", data_timestamp)
        
        print(f"Node {self.node_id} starting consensus session.")
        self.session_nodes = self.select_session_nodes()
        print(f"session nodes: {self.session_nodes}")
        
        consensus_success = self.perform_consensus_with_timeout()

        if consensus_success:
            self.commit_block()
        else:
            print("Consensus failed. Block not committed.")

        self.active_blocking = False
        self.session_start_time = time.time()

    def select_session_nodes(self):
        """Select a subset of nodes for the consensus session."""
        #total_nodes = list(self.peers.keys()) + [self.node_id]
        #session_nodes = random.sample(total_nodes, max(1, len(total_nodes) // 2))

        session_nodes = set([data["node_id"] for data in self.trades[:self.last_received+1]])

        return list(session_nodes)

    def perform_consensus_with_timeout(self):
        """Perform consensus and wait for verification with a timeout."""
        start_time = time.time()

        random_numbers = {}
        random_list =list(range(len(self.session_nodes)))
        random.shuffle(random_list)

        shuffled_nodes = derangement(self.session_nodes)

        print(f"random_list, {random_list}")
        print(f"shuffled_nodes, {shuffled_nodes}")

        for i, node in enumerate(self.session_nodes):
            group_trades = [data for data in self.trades[:self.last_received+1] if data["node_id"]==node]
            #random_numbers[node] = random.randint(1, len(group_trades))
            random_numbers[node] = random_list[i]

            self.request_consensus(shuffled_nodes[i], node, random_numbers[node], group_trades)

        random_numbers_list = [(key,value) for key, value in random_numbers.items()]
        print(f"random_numbers_list {random_numbers_list}")

        while time.time() - start_time < self.consensus_timeout:
            # Simulate receiving responses from session nodes
            print(f"responses: {self.consensus_response}")
            success_count = 0
            responses = self.consensus_response
            for item in responses:
                if (item[0], item[1]) in random_numbers_list:
                    if item[2]:
                        success_count += 1
                    else:
                        self.trades = [trade for trade in self.trades if trade["node_id"]==item[0]]
                        return False
            if success_count > len(self.session_nodes)//2:
                print(f"Received majority responses: {responses}")
                return True

            time.sleep(0.005)  # Wait a bit before checking again

        print("Consensus timeout reached. Not all responses received.")
        return False

    def request_consensus(self, rec_node_id, send_node_id, random_number, data):
        """Send a consensus request to a node."""
        message = {
            "type": "consensus_request", 
            "orderer_id": self.node_id, 
            "send_node_id": send_node_id,
            "random_number":random_number, 
            "data":data}

        if rec_node_id == self.node_id:
            self.handle_consensus_request(message)
            print(f"request_consensus {message}")
        else:
            self.send_message(rec_node_id, message)

    def handle_consensus_request(self, message):
        """Handle a consensus request."""
        random_number = message["random_number"]
        send_node_id = message["send_node_id"]
        response = {
            "type": "consensus_response",
            "node_id": self.node_id,
            "send_node_id": send_node_id,
            "random_number": random_number,
            "verify": True,
        }
        if self.node_id == self.orderer_id:
            self.handle_consensus_response(response)
        else:
            self.send_message(message["orderer_id"], response)
        print("handle_consensus_request: Assume verification true")

    def handle_consensus_response(self, message):
        random_number = message["random_number"]
        node_id = message["send_node_id"]
        verify = message["verify"]
        self.consensus_response.add((node_id, random_number, verify))

    def commit_block(self):
        """Commit the block to the ledger."""
        block = {
            "block_id": len(self.ledger),
            "node_id": self.node_id,
            "trades": self.trades[:self.last_received+1],
            "timestamp": time.time(),
        }
        #self.last_commit = self.last_received
        self.ledger.append(block)
        self.trades = self.trades[self.last_received+1:]  # Clear the trade pool
        print(f"Block committed: {block}")

        for data in block["trades"]:
            print(f"Block committed, data in trades: {data}")
            data_timestamp = data["time"]
            self.log_evaluation("COMMIT", data_timestamp)


        # Broadcast commit info to all session nodes
        for node_id in self.peers:
            self.send_message(node_id, {"type": "commit", "block": block})

    def send_message(self, peer_id, message):
        print(f"SEND MESSAGE: {message}")
        self.log_evaluation("SEND_MESSAGE", "")
        host, port = self.peers[peer_id]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((host, port))
                s.sendall(json.dumps(message).encode("utf-8"))
            #except ConnectionRefusedError:
            except:
                print(f"Node {self.node_id} could not connect to Node {peer_id}.")

    def handle_commit(self, message):
        """Handle a commit message from the orderer."""
        block = message["block"]
        block_id = block["block_id"]

        # Check if the block is already committed
        if any(b["block_id"] == block_id for b in self.ledger):
            print(f"Node {self.node_id} already committed block {block_id}. Ignoring.")
            return

        # Add block to local ledger
        self.ledger.append(block)
        print(f"Node {self.node_id} committed block {block_id} from orderer.")
        for data in block["trades"]:
            data_timestamp = data["time"]
            self.log_evaluation("COMMIT", data_timestamp)

    def apply_logs_to_state_machine(self):
        """Apply committed log entries to the state machine (write to a local file)."""
        with open(f"node_{self.node_id}_messages.log", "a") as file:
            while not self.stop_event.is_set():
                if self.last_applied < len(self.ledger)-1:
                    last_applied_index = self.last_applied
                    for index in range(last_applied_index + 1, len(self.ledger)):
                        
                        block  = self.ledger[index]
                        block_trades = block["trades"]
                        print(f"apply_logs_to_state_machine, block {block}, block len {len(block_trades)}, index {index}")
                        for trade in block["trades"]:
                            print(f"apply_logs_to_state_machine, trade {trade}")
                            file.write(json.dumps(trade) + "\n")  # Write log entry to the file
                            file.flush()
                        self.last_applied = index  # Update lastApplied
                time.sleep(0.1)  # Check periodically for new committed entries


    def stop(self):
        """Stop the node."""
        self.stop_event.set()
        if self.socket:
            self.socket.close()


def derangement(lst):
    n = len(lst)
    indices = list(range(n))
    while True:
        random.shuffle(indices)
        if all(i != indices[i] for i in range(n)):
            return [lst[indices[i]] for i in range(n)]

# Example usage
if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    total_nodes_num = int(sys.argv[4])

    with open(sys.argv[3], 'r') as f:
        peers = json.load(f)

    converted_peers = {key: peers[str(key)] for key in range(1,total_nodes_num+1)}

    del converted_peers[node_id]
    print(f"converted_peers {converted_peers}")

    node = PoBTNode(node_id, converted_peers, port)
    try:
        node.start()
    except KeyboardInterrupt:
        node.stop()
