import socket
import time
import threading
import queue
import select


def csv_to_topic(self, matrix_lines):
    local_matrix_lines = matrix_lines
    del local_matrix_lines[1]
    del local_matrix_lines[1]
    del local_matrix_lines[1]

    topic_dict = {}
    matrix_array = []

    for i in range(0, len(local_matrix_lines)):
        local_matrix_lines[i] = local_matrix_lines[i].strip("\n")

    for i in range(0, len(local_matrix_lines)):
        matrix_array.append(local_matrix_lines[i].split(";"))

    # remove topic description
    for i in range(0, len(matrix_array)):
        matrix_array[i].pop()

    # make dict
    for i in range(1, len(matrix_array)):
        msg_name = matrix_array[i][0]
        msg_name = msg_name.lower()  # ensure that all topics are lower case

        if len(msg_name) > self.topic_length:
            print(f"topic: {msg_name} too long, set topic_length= on both server and client side, "
                  f"or reduce topic length")
            raise ValueError

        topic_dict[msg_name] = []

        for j in range(1, (len(matrix_array[i]))):
            try:
                if int(matrix_array[i][j]) == 1:
                    topic_dict[msg_name].append(matrix_array[0][j])
            except ValueError:
                pass

    return topic_dict


def generate_node_dict(topic_matrix):
    node_dict = {}
    node_name_list = topic_matrix[0].split(";")
    del node_name_list[0]
    node_name_list[len(node_name_list) - 1] = node_name_list[len(node_name_list) - 1].strip("\n")
    if node_name_list[len(node_name_list) - 1].lower() == "description":
        del node_name_list[len(node_name_list) - 1]

    server_receive_port_list = topic_matrix[1].split(";")
    del server_receive_port_list[0]

    server_transmit_port_list = topic_matrix[2].split(";")
    del server_transmit_port_list[0]

    node_ip_list = topic_matrix[3].split(";")
    del node_ip_list[0]

    for i in range(0, len(node_name_list)):
        node_dict[node_name_list[i]] = {'IP': None, 'SERVER_RECEIVE_PORT': None, 'SERVER_TRANSMIT_PORT': None,
                                        'CLIENT_ADDRESS': None, 'QUEUE': queue.Queue()}
        node_dict[node_name_list[i]]["IP"] = node_ip_list[i]
        node_dict[node_name_list[i]]["SERVER_RECEIVE_PORT"] = int(server_receive_port_list[i])
        node_dict[node_name_list[i]]["SERVER_TRANSMIT_PORT"] = int(server_transmit_port_list[i])

    return node_dict


def server_get_connection(self, server_socket, node_name):
    timeout_in_seconds = 0.1
    while self.signal:
        ready = select.select([server_socket], [], [], timeout_in_seconds)
        if ready[0]:
            msg, client_address = server_socket.recvfrom(1024)
            if self.debug:
                print('GOT connection from ', msg.decode('utf-8'))
            self.server_node_dict[node_name]["CLIENT_ADDRESS"] = client_address

    server_socket.close()
    if self.debug:
        print(f"server_get_connection closed {node_name}", node_name)
    self.open_connections -= 1


def server_message_transmitter(self, node_name):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.buffer_size)
    host_ip = self.server_node_dict[node_name]["IP"]
    port = self.server_node_dict[node_name]["SERVER_TRANSMIT_PORT"]
    socket_address = (host_ip, port)
    server_socket.bind(socket_address)
    self.open_connections += 1

    t_transmitter = threading.Thread(target=server_get_connection, args=(self, server_socket, node_name,))
    t_transmitter.daemon = True
    t_transmitter.start()

    while self.signal:
        if self.server_node_dict[node_name]["CLIENT_ADDRESS"]:
            message = self.server_node_dict[node_name]["QUEUE"].get()
            if message != "stop":
                server_socket.sendto(message, self.server_node_dict[node_name]["CLIENT_ADDRESS"])
        else:
            time.sleep(0.001)

    server_socket.close()
    if self.debug:
        print(f"server_message_transmitter closed {node_name}", node_name)
    self.open_connections -= 1


def server_message_receiver(self, node_name):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.buffer_size)
    host_ip = self.server_node_dict[node_name]["IP"]
    port = self.server_node_dict[node_name]["SERVER_RECEIVE_PORT"]
    socket_address = (host_ip, port)
    server_socket.bind(socket_address)
    self.open_connections += 1
    timeout_in_seconds = 0.1

    while self.signal:
        ready = select.select([server_socket], [], [], timeout_in_seconds)
        if ready[0]:
            msg, client_address = server_socket.recvfrom(self.buffer_size)

            if self.debug:
                print(msg)
            self.server_incoming_msg_queue.put(msg)

    server_socket.close()
    self.open_connections -= 1
    if self.debug:
        print(f"server_message_receiver closed: {node_name}", node_name)


def server_message_handler(self):
    topic_start = self.header_length
    topic_end = self.header_length + self.topic_length

    while self.signal:
        msg = self.server_incoming_msg_queue.get()
        if msg != "stop":
            if len(msg) >= topic_end:
                topic = msg[topic_start:topic_end].decode('utf-8').strip(" ")
                if topic in self.server_topic_dict:
                    for i in range(0, len(self.server_topic_dict[topic])):
                        topic_key = self.server_topic_dict[topic][i]
                        if topic_key in self.server_node_dict:
                            if not self.server_node_dict[topic_key]["QUEUE"].empty():
                                remove_data = self.server_node_dict[topic_key]["QUEUE"].get()
                            self.server_node_dict[topic_key]["QUEUE"].put(msg)
        else:
            for i in range(0, len(self.server_topic_dict[topic])):
                if not self.server_node_dict[topic_key]["QUEUE"].empty():
                    remove_data = self.server_node_dict[topic_key]["QUEUE"].get()
                self.server_node_dict[topic_key]["QUEUE"].put("stop")


def client_message_transmitter(self):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.buffer_size)
    self.open_connections += 1
    while self.signal:
        msg_list = self.client_msg_to_be_send.get()
        if not msg_list == "stop":
            if type(msg_list[2]) is str:
                msg_list[2] = msg_list[2].encode('utf-8')
            message_header = f"{len(msg_list[2]):<{self.header_length}}".encode('utf-8')
            if self.send_timestamp:
                msg_timestamp = f"{str(msg_list[0]):<{self.timestamp_length}}".encode('utf-8')
            message_topic = f"{msg_list[1]:<{self.topic_length}}".encode('utf-8')

            if self.send_sender_name:
                sender_name = f"{self.node_name:<{self.sender_length}}".encode('utf-8')

            combined_message = message_header + message_topic

            if self.send_timestamp:
                combined_message = combined_message + msg_timestamp

            if self.send_sender_name:
                combined_message = combined_message + sender_name

            combined_message = combined_message + msg_list[2]
            client_socket.sendto(combined_message, (self.ip, self.server_recv_port))
    client_socket.close()
    self.open_connections -= 1


def client_message_receiver(self):
    msg_dict = {}
    header_end = self.header_length

    topic_start = self.header_length
    topic_end = self.header_length + self.topic_length

    if self.send_timestamp:
        timestamp_start = self.header_length + self.topic_length
        timestamp_end = self.header_length + self.topic_length + self.timestamp_length

    if self.send_sender_name:
        if self.send_timestamp:
            sender_name_start = self.header_length + self.topic_length + self.timestamp_length
            sender_name_end = self.header_length + self.topic_length + self.timestamp_length + self.sender_length
        else:
            sender_name_start = self.header_length + self.topic_length
            sender_name_end = self.header_length + self.topic_length + self.sender_length

    if self.send_timestamp and self.send_sender_name:
        msg_start = self.header_length + self.topic_length + self.timestamp_length + self.sender_length

    elif self.send_timestamp:
        msg_start = self.header_length + self.topic_length + self.timestamp_length

    elif self.send_sender_name:
        msg_start = self.header_length + self.topic_length + self.sender_length

    else:
        msg_start = self.header_length + self.topic_length

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.buffer_size)
    self.open_connections += 1

    message = self.node_name.encode('utf-8')
    client_socket.sendto(message, (self.ip, self.server_transmit_port))
    timeout_in_seconds = 0.1
    while self.signal:
        ready = select.select([client_socket], [], [], timeout_in_seconds)
        if ready[0]:
            msg, _ = client_socket.recvfrom(self.buffer_size)
            if len(msg) > self.header_length:
                try:
                    msg_length = int(msg[:header_end].decode('utf-8').strip(" "))
                    if len(msg) == msg_start + msg_length:
                        msg_dict["topic"] = msg[topic_start:topic_end].decode('utf-8').strip(" ")

                        if self.send_timestamp:
                            timestamp = float(msg[timestamp_start:timestamp_end].decode('utf-8').strip(" "))
                            msg_dict["timestamp"] = timestamp
                            msg_dict["recv_timestamp"] = time.time()

                        if self.send_sender_name:
                            sender = msg[sender_name_start:sender_name_end].decode('utf-8').strip(" ")
                            msg_dict["sender"] = sender

                        message = msg[msg_start:]
                        msg_dict["message"] = message
                        msg_dict["valid"] = True
                        self.client_incoming_msg_queue.put(msg_dict)

                    else:
                        if self.debug:
                            print("message corrupt")
                except:
                    if self.debug:
                        print("message corrupt")
                    continue

    client_socket.close()
    if self.debug:
        print("receiver socket closed")
    self.open_connections -= 1


class Server(threading.Thread):
    def __init__(self, topic_matrix, header_length=10, topic_length=20, buffer_size=65535, debug=False):
        threading.Thread.__init__(self)
        self.signal = True
        self.server_incoming_msg_queue = queue.Queue()
        self.server_node_dict = generate_node_dict(topic_matrix)
        self.buffer_size = buffer_size
        self.header_length = header_length
        self.topic_length = topic_length
        self.timestamp_length = 20
        self.server_topic_dict = csv_to_topic(self, topic_matrix)
        self.debug = debug
        self.open_connections = 0

    def start_server(self):
        for key in self.server_node_dict:
            t1 = threading.Thread(target=server_message_transmitter, args=(self, key,))
            t1.daemon = True
            t1.start()

            t2 = threading.Thread(target=server_message_receiver, args=(self, key,))
            t2.daemon = True
            t2.start()

        t3 = threading.Thread(target=server_message_handler, args=(self,))
        t3.daemon = True
        t3.start()

    def close_server(self):
        self.signal = False
        while self.open_connections > 0:
            if self.debug:
                print("waiting for connection to close")
            time.sleep(0.1)

        if self.debug:
            print("open server connections: ", self.open_connections)


class Client(threading.Thread):
    def __init__(self, ip=None, server_recv_port=None, server_transmit_port=None, header_length=10, topic_length=20,
                 buffer_size=65535, send_timestamp=False, node_name=None, debug=False, send_sender_name=False,
                 sender_length=20):
        threading.Thread.__init__(self)

        self.signal = True
        self.ip = ip
        self.server_recv_port = server_recv_port
        self.server_transmit_port = server_transmit_port
        self.header_length = header_length
        self.topic_length = topic_length
        self.timestamp_length = 20
        self.buffer_size = buffer_size
        self.send_timestamp = send_timestamp
        self.send_sender_name = send_sender_name
        self.sender_length = sender_length
        self.status = None
        self.node_name = node_name
        self.client_msg_to_be_send = queue.Queue()
        self.client_incoming_msg_queue = queue.Queue()
        self.debug = debug
        self.open_connections = 0
        self.max_message_length = 0

    def start_client(self):
        if self.server_transmit_port and self.server_recv_port and self.node_name:
            if self.send_sender_name:
                if len(self.node_name) > self.sender_length:
                    print(f"node name: {self.node_name} too long either reduce node name or set sender_length=")
                    raise ValueError

            self.max_message_length = self.buffer_size - self.topic_length
            if self.send_timestamp:
                self.max_message_length -= self.timestamp_length
            if self.send_sender_name:
                self.max_message_length -= self.sender_length

            self.node_name = self.node_name.upper()
            self.signal = True
            self.status = True
            t1 = threading.Thread(target=client_message_transmitter, args=(self,))
            t1.daemon = True
            t1.start()

            t2 = threading.Thread(target=client_message_receiver, args=(self,))
            t2.daemon = True
            t2.start()

        else:
            if not self.server_recv_port:
                print("server receive port required")
                raise ValueError

            if not self.server_transmit_port:
                print("server transmit port required")
                raise ValueError

            if not self.node_name:
                print("node name required to connect")
                raise ValueError

    def close_client(self):
        self.signal = False
        self.client_msg_to_be_send.put("stop")

        while self.open_connections > 0:
            if self.debug:
                print("waiting for connections to close")
                time.sleep(0.1)

        if self.debug:
            print("open client connections: ", self.open_connections)
        self.status = None

    def incoming_msg(self, blocking=True):
        if blocking:
            incoming_msg_dict = self.client_incoming_msg_queue.get()
        else:
            if not self.client_incoming_msg_queue.empty():
                incoming_msg_dict = self.client_incoming_msg_queue.get()
            else:
                incoming_msg_dict = {"timestamp": 0.0, "recv_timestamp": 0.0, "topic": "", "message": "", "valid": False}
        return incoming_msg_dict

    def send_msg(self, topic, message):
        if len(topic) > self.topic_length:
            print(f"topic: {topic} too long, set topic_length= on both server and client side, "
                  f"or reduce topic length")
            raise ValueError

        if len(message) > self.max_message_length:
            print(f"message: {message} too long, increase buffer size, or reduce message length")
            raise ValueError

        timestamp = time.time()
        msg_list = [timestamp, topic.lower(), message]
        self.client_msg_to_be_send.put(msg_list)
