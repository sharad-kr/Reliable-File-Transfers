import sys
import socket
from time import sleep, perf_counter
import hashlib
from collections import OrderedDict

if len(sys.argv) < 3:
    print("Server IP Address and port required as arguments")
    exit(1)

SERVER_IP = sys.argv[1]
SERVER_PORT = int(sys.argv[2])
MAX_BYTES = 1448
SOCKET_RECEIVE_BYTES = 2048

SLEEP_SECONDS = 0.003
TIMEOUT_SECONDS = 0.005
SLEEP_SQUISH_SECONDS = 0.01

INITIAL_BURST_SIZE = 4

size_message = "SendSize\nReset\n\n"

successful_requests = 0
failed_requests = 0
squished_requests = 0

MAX_SIZE = -1
MAX_ATTEMPTS = 100

remaining_offsets = OrderedDict()
file = []


def send_requests(request_offsets):
    for offset in request_offsets:
        num_bytes = min(MAX_BYTES, MAX_SIZE - offset)
        request_message = f"Offset: {offset}\nNumBytes: {num_bytes}\n\n"
        try:
            sock.sendto(request_message.encode("utf-8"), (SERVER_IP, SERVER_PORT))
            sleep(SLEEP_SECONDS)
        except:
            print("Error sending request to server")
            exit(1)


def receive_requests(burst_size):
    global squished_requests, successful_requests, failed_requests
    accepted_offsets = []
    for _ in range(burst_size):
        try:
            response_bytes, addr = sock.recvfrom(SOCKET_RECEIVE_BYTES)
            response_data = response_bytes.decode("utf-8")
            response_tokens = response_data.split("\n")
            response_offset = int(response_tokens[0].split(":")[1])
            content = ""
            if (response_tokens[2] == "Squished"):
                content = "\n".join(response_tokens[4:])
                squished_requests += 1
                sleep(SLEEP_SQUISH_SECONDS)
            else:
                content = "\n".join(response_tokens[3:])
            if (len(file[response_offset // MAX_BYTES]) == 0):
                accepted_offsets.append(response_offset)
                file[response_offset // MAX_BYTES] = content
                successful_requests += 1
        except socket.timeout:
            failed_requests += 1
    return accepted_offsets


burst_size = INITIAL_BURST_SIZE
max_burst_size = INITIAL_BURST_SIZE
min_burst_size = INITIAL_BURST_SIZE


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(TIMEOUT_SECONDS)
sock.sendto(size_message.encode("utf-8"), (SERVER_IP, SERVER_PORT))

for _ in range(MAX_ATTEMPTS):
    try:
        size_data, addr = sock.recvfrom(SOCKET_RECEIVE_BYTES);
        MAX_SIZE = int(size_data.decode("utf-8").split(":")[1])
        break;
    except:
        pass

if (MAX_SIZE == -1):
    print("Unable to receive data from the server")
    exit(1)
print(f"Max Size: {MAX_SIZE}")

for i in range(0, MAX_SIZE, MAX_BYTES):
    remaining_offsets[i] = 0
    file.append("")

try:
    while len(remaining_offsets) > 0:
        accepted_offsets = []
        print(f"Remaining offsets: {len(remaining_offsets)}")
        current_offsets = []

        for offset in remaining_offsets:
            current_offsets.append(offset)
            if (len(current_offsets) >= burst_size):
                send_requests(current_offsets)
                accepted_offsets_in_burst = receive_requests(len(current_offsets))
                for accepted_offset in accepted_offsets_in_burst:
                    accepted_offsets.append(accepted_offset)
                if (len(accepted_offsets_in_burst) == len(current_offsets)):
                    burst_size += 1
                    max_burst_size = max(max_burst_size, burst_size)
                else:
                    burst_size = max(burst_size // 2, 1)
                    min_burst_size = min(min_burst_size, burst_size)
                current_offsets.clear()

        if (len(current_offsets) > 0):
            send_requests(current_offsets)
            accepted_offsets_in_burst = receive_requests(len(current_offsets))
            for accepted_offset in accepted_offsets_in_burst:
                accepted_offsets.append(accepted_offset)
            if (len(accepted_offsets_in_burst) == len(current_offsets)):
                burst_size += 1
                max_burst_size = max(max_burst_size, burst_size)
            else:
                burst_size = max(burst_size // 2, 1)
                min_burst_size = min(min_burst_size, burst_size)
            current_offsets.clear()

        for offset in accepted_offsets:
            remaining_offsets.pop(offset)

except KeyboardInterrupt:
    print(f"Remaining offsets: {len(remaining_offsets)}")
    exit(1)


file_data = "".join(file)
print(f"Bytes of data received: {len(file_data)}")

file_data_hash = hashlib.md5(file_data.encode("utf-8")).hexdigest()
print(f"MD5 hash: {file_data_hash}")

submit_message = f"Submit: 2021CS10099_2021CS10581@slowbrains\nMD5: {file_data_hash}\n\n"
sock.sendto(submit_message.encode("utf-8"), (SERVER_IP, SERVER_PORT))


while True:
    try:
        response_data, addr = sock.recvfrom(SOCKET_RECEIVE_BYTES);
        submit_response = response_data.decode("utf-8")
        if submit_response.__contains__("Result: ") and submit_response.__contains__("Time: ") and submit_response.__contains__("Penalty: "):
            print("---------------------------------")
            print(submit_response[:-2])
            print("---------------------------------")
            break
    except:
        pass


print(f"Successful requests: {successful_requests}")
print(f"Failed requests: {failed_requests}")
print(f"Squished requests: {squished_requests}")
print(f"Final Burst Size: {burst_size}")
print(f"Minimum Burst Size: {min_burst_size}")
print(f"Maximum Burst Size: {max_burst_size}")
