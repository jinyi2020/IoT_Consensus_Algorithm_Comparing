import socket
import threading
import json
import sys
import random
import time
from enum import Enum
import psutil
import tracemalloc
import signal


class State(Enum):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"

class RaftNode:
    def __init__(self, node_id, peers, port):
        self.node_id = node_id
        self.peers = peers  # {peer_id: (host, port)}
        self.port = port
        self.state = State.FOLLOWER
        self.term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {peer_id: 0 for peer_id in self.peers}
        self.match_index = {peer_id: -1 for peer_id in self.peers}
        self.leader_id = None
        
        self.heartbeat_interval = 1
        self.votes_received = 0
        self.socket = None
        self.stop_event = threading.Event()
        self.election_timeout = time.time() +  random.uniform(3, 7)

        self.evaluation_log_file = f"node_{self.node_id}_evaluation.log"
        with open(self.evaluation_log_file, "w") as f:
            f.write("NodeID,Timestamp,Action,DataTime,CPU,MEMORY,VIEW\n")

        tracemalloc.start()

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

        with open(self.evaluation_log_file, "a") as f:
            f.write(f"{self.node_id},{timestamp},{action},{datatime},{cpu_usage},{memory_usage},{view}\n")
            f.flush()

    def set_election_time_out(self):
        new_election_timeout = time.time() +  random.uniform(2, 5)
        self.election_timeout = max(self.election_timeout, new_election_timeout)

    def start(self):
        threading.Thread(target=self.run_server).start()
        threading.Thread(target=self.run_raft_state_manager).start()
        threading.Thread(target=self.generate_iot_message, daemon=True).start()
        threading.Thread(target=self.apply_logs_to_state_machine, daemon=True).start()

    def generate_iot_message(self):
        """Generate and send IoT data to the leader periodically."""
        while not self.stop_event.is_set():
            # Generate IoT data
            data = {
                "node_id": self.node_id,
                "time": round(time.time(),2),
                "temperature": round(random.uniform(15, 30),2),
                "humidity": round(random.uniform(30, 60),2),
                "air_quality": round(random.uniform(50, 100),2),
            }

            _t = data["time"]
            print(f"iot data generated at {_t}")
            self.log_evaluation("GENERATE", _t)

            if self.state == State.LEADER:
                self.log.append({"term": self.term, "data": data})
                data_timestamp = data["time"]
                print( f"consensus for message { data_timestamp } start")
                self.log_evaluation("BROADCAST", data_timestamp)
                self.broadcast_append_entries()

            elif self.leader_id is not None:
                # Send data to the leader
                self.send_message(self.leader_id, {
                    "type": "data",
                    "data": data
                })       
            time.sleep(5)  # Send IoT data every 5 seconds

    def apply_logs_to_state_machine(self):
        """Apply committed log entries to the state machine (write to a local file)."""
        with open(f"node_{self.node_id}_messages.log", "a") as file:
            while not self.stop_event.is_set():
                if self.last_applied < self.commit_index:
                    last_applied_index = self.last_applied
                    for index in range(last_applied_index + 1, self.commit_index + 1):
                        
                        entry = self.log[index]
                        print(f"apply_logs_to_state_machine, entry {entry}, log len {len(self.log)}, index {index}")
                        self.log_evaluation("APPLY", entry["data"]["time"])
                        file.write(json.dumps(entry) + "\n")  # Write log entry to the file
                        file.flush()
                        self.last_applied = index  # Update lastApplied
                time.sleep(0.1)  # Check periodically for new committed entries

    def run_server(self):
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
                #_dec = data.decode("utf-8")
                #print(f"before message json load: f{_dec}")
                message = json.loads(data.decode("utf-8"))
                
                self.handle_message(message)

    def handle_message(self, message):
        print(f"REVEIVE MESSAGE")
        self.log_evaluation("RECEIVE_MESSAGE", "")
        if message["type"] == "vote_request":
            self.handle_vote_request(message)
        elif message["type"] == "append_entries":
            self.handle_append_entries(message)
        elif message["type"] == "data":
            self.handle_data(message)
        elif message["type"] == "append_entries_response":
            self.handle_entires_response(message)
        elif message["type"] == "vote_response":
            self.handle_vote_response(message)

    def handle_append_entries(self, message):
        term = message["term"]
        leader_id = message["leader_id"]
        prev_log_index = message["prev_log_index"]
        prev_log_term = message["prev_log_term"]
        entries = message["entries"]
        leader_commit = message["leader_commit"]

        print(f"handle append entries: term {term}, leader_id {leader_id}, prev_log_index {prev_log_index}, prev_log_term {prev_log_term}, entries_len {len(entries)}, leader_commit {leader_commit}")

        for entry in entries:
            self.log_evaluation("RECEIVE_ENTRY", entry["data"]["time"])

        if term < self.term:
            self.send_message(leader_id, {"type": "append_entries_response", "term": self.term, "peer_id":self.node_id, "success": False})
            print(f"handle append entries: false")
            return

        self.set_election_time_out()
        

        self.state = State.FOLLOWER
        self.term = term
        self.leader_id = leader_id

        if len(self.log)-1 < prev_log_index or (prev_log_index >= 0 and self.log[prev_log_index]["term"] != prev_log_term):
            self.send_message(leader_id, {"type": "append_entries_response", "term": self.term, "peer_id":self.node_id, "success": False})
            print(f"handle append entries: false")
            return

        self.log = self.log[:prev_log_index + 1] + entries
        pre_commit_index = self.commit_index
        self.commit_index = min(leader_commit, len(self.log)-1)
        if self.commit_index != pre_commit_index:
            print(f"COMMIT, pre index {pre_commit_index}, new index {self.commit_index}")
            for index in range(pre_commit_index + 1, self.commit_index + 1):
                print(f"commit index {self.commit_index}, index {index}, log len {len(self.log)}")
                self.log_evaluation("COMMIT", self.log[index]["data"]["time"],term)
        self.send_message(leader_id, {"type": "append_entries_response", "term": self.term, "peer_id":self.node_id, "success": True, "matchIndex":len(self.log)-1})
        print(f"handle append entries: true")

    def handle_entires_response(self, message):
        term = message["term"]
        peer_id = message["peer_id"]
        success = message["success"]

        print(f"handle entries response: term {term}, peer_id {peer_id}, success {success}")

        if term > self.term:
            self.voted_for = None
            self.state = State.FOLLOWER
            self.term = term
            return

        if success == False:
            self.next_index[peer_id] -= 1
            self.send_append_entries(peer_id, self.log)
            return

        if success == True:
            matchIndex = message["matchIndex"]
            self.match_index[peer_id] = matchIndex
            self.next_index[peer_id] = matchIndex + 1

            print(f"handle entries response: matchIndex, {matchIndex}, len_log {len(self.log)}")

            if self.next_index[peer_id] < len(self.log):
                self.send_append_entries(peer_id, self.log)

            all_match_index = sorted(self.match_index.values())[len(self.peers) // 2]
            #self.commit_index = max(all_match_index, self.commit_index)

            # Check the term of the entry at this index
            if self.commit_index < all_match_index and self.log[all_match_index]["term"] == self.term:
                print(f"check match index: {self.match_index}")
                print(f"all match index: {all_match_index}")
                print(f"log len {len(self.log)}")
                prev_commit_index = self.commit_index
                self.commit_index = all_match_index
                print(f"Entry at index {self.commit_index} committed.")
                data_timestamp = self.log[self.commit_index]["data"]["time"]
                print( f"consensus for message { data_timestamp } end")
                for index in range(prev_commit_index+1, self.commit_index+1):
                    data_timestamp = self.log[index]["data"]["time"]
                    self.log_evaluation("COMMIT", data_timestamp,term)
                


    def handle_data(self, message):
        if self.state == State.LEADER:
            self.log.append({"term": self.term, "data": message["data"]})
            data_timestamp = message["data"]["time"]
            print( f"consensus for message { data_timestamp } start")
            self.log_evaluation("BROADCAST", data_timestamp)
            self.broadcast_append_entries(self.log)
            
            

    def send_message(self, peer_id, message):
        print(f"SEND MESSAGE")
        self.log_evaluation("SEND_MESSAGE", "")
        host, port = self.peers[peer_id]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((host, port))
                s.sendall(json.dumps(message).encode("utf-8"))
            #except ConnectionRefusedError:
            except:
                print(f"Node {self.node_id} could not connect to Node {peer_id}.")

    def send_append_entries(self, peer_id, entries=[]):
        next_ind = self.next_index[peer_id]
        end_ind = min(next_ind + 5, len(self.log))
        prev_term = self.log[next_ind -1]["term"] if self.log else 0
        entries_len = len(entries[next_ind:end_ind] if entries else [])
        print(f"send append entries: term {self.term}, peer_id {peer_id}, leader_id {self.node_id}, prev_log_index {next_ind -1}, prev_log_term, {prev_term}, entries_len {entries_len}, leader_commit {self.commit_index}")
        threading.Thread(target=self.send_message, args=(peer_id, {
                "type": "append_entries",
                "term": self.term,
                "leader_id": self.node_id,
                "prev_log_index": next_ind -1,
                "prev_log_term": self.log[next_ind -1]["term"] if self.log else 0,
                "entries": entries[next_ind:end_ind] if entries else [],
                "leader_commit": self.commit_index,
            })).start()

    def broadcast_append_entries(self, entries=[]):
        for peer_id in self.peers:
            self.send_append_entries(peer_id, entries)

    def is_log_up_to_date(self, last_log_index, last_log_term):
        if len(self.log) == 0:
            return True
        return last_log_term > self.log[-1]["term"] or (last_log_term == self.log[-1]["term"] and last_log_index >= len(self.log)-1)

    def run_raft_state_manager(self):
        while not self.stop_event.is_set():
            if self.state == State.FOLLOWER:
                self.run_follower()
            elif self.state == State.CANDIDATE:
                self.run_candidate()
            elif self.state == State.LEADER:
                self.run_leader()

    def run_follower(self):
        try:
            print(f"run follower, term {self.term}")
            self.set_election_time_out()
            while self.state == State.FOLLOWER:
                current_time = time.time()
                if current_time > self.election_timeout:
                    self.state = State.CANDIDATE
                else:
                    time.sleep(self.election_timeout - current_time)
                      
        except:
            pass

    def run_candidate(self):
        print(f"run candidate, term {self.term}")
        self.set_election_time_out()
        self.term += 1
        self.voted_for = self.node_id
        self.votes_received = 1
        for peer_id in self.peers:
            self.send_vote_request(peer_id)

        current_time = time.time()
        while (self.state == State.CANDIDATE and  current_time<self.election_timeout):
            current_time = time.time()
            time.sleep(min(0.1, self.election_timeout))

    def send_vote_request(self, peer_id):
        prev_term = self.log[-1]["term"] if self.log else 0
        print(f"send vote request, peer_id {peer_id}, term {self.term}, candidate_id {self.node_id}, last_log_index {len(self.log)-1}, last_log_term {prev_term}")
        threading.Thread(target=self.send_message, args=(peer_id, {
            "type": "vote_request",
            "term": self.term,
            "candidate_id": self.node_id,
            "last_log_index": len(self.log)-1,
            "last_log_term": self.log[-1]["term"] if self.log else 0,
        })).start()

    def handle_vote_request(self, message):
        term = message["term"]
        candidate_id = message["candidate_id"]
        last_log_index = message["last_log_index"]
        last_log_term = message["last_log_term"]

        print(f"handle vote request: {message}")

        if term < self.term:
            self.send_message(candidate_id, {"type": "vote_response", "term": self.term, "vote_granted": False})
            print(f"handle vote request: false")
            return

        if term > self.term:
            self.voted_for = None
            self.state = State.FOLLOWER
            self.term = term

        if (self.voted_for is None or self.voted_for == candidate_id) and self.is_log_up_to_date(last_log_index, last_log_term):
            self.voted_for = candidate_id
            self.term = term
            #self.state = State.FOLLOWER
            self.send_message(candidate_id, {"type": "vote_response", "term": self.term, "vote_granted": True})
            print(f"handle vote request: true")

    def handle_vote_response(self, message):
        term = message["term"]
        granted = message["vote_granted"]

        print(f"handle vote response: {message}")

        if term > self.term:
            self.voted_for = None
            self.state = State.FOLLOWER
            self.term = term
            return

        if (self.state == State.CANDIDATE and granted and term == self.term):
            self.votes_received += 1
            if self.votes_received > (len(self.peers) + 1)/2:
                self.state = State.LEADER
                        

    def run_leader(self):
        print(f"run leader, term {self.term}")
        self.log_evaluation("NODE_LEADER","")
        self.next_index = {peer_id: len(self.log) for peer_id in self.peers}
        self.match_index = {peer_id: -1 for peer_id in self.peers}

        while self.state == State.LEADER:
            self.broadcast_append_entries()
            time.sleep(self.heartbeat_interval)

    def stop(self):
        self.log_evaluation("NODE_STOP","")
        tracemalloc.stop()
        self.stop_event.set()
        if self.socket:
            self.socket.close()


def handle_sigterm(signal_number, frame):
    print("SIGTERM received, shutting down...")
    node.stop()
    sys.exit(0)  # Exit the program cleanly

if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    #peers = json.loads(sys.argv[3])
    total_nodes_num = int(sys.argv[4])

    with open(sys.argv[3], 'r') as f:
        peers = json.load(f)

    converted_peers = {key: peers[str(key)] for key in range(1,total_nodes_num+1)}

    del converted_peers[node_id]
    print(f"converted_peers {converted_peers}")

    #converted_peers = {int(key): value for key, value in peers.items()}

    # Register SIGTERM handler
    signal.signal(signal.SIGTERM, handle_sigterm)

    node = RaftNode(node_id, converted_peers, port)
    try:
        node.start()
    except KeyboardInterrupt:
        node.stop()