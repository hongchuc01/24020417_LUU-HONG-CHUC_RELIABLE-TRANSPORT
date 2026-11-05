import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

START = 0
END = 1
DATA = 2
ACK = 3

HEADER_SIZE = 16
MAX_PACKET_SIZE = 1472
MAX_DATA_SIZE = MAX_PACKET_SIZE - HEADER_SIZE
TIMEOUT = 0.5


def make_packet(pkt_type, seq_num, data=b""):
    header = PacketHeader(
        type=pkt_type,
        seq_num=seq_num,
        length=len(data),
        checksum=0
    )

    header.checksum = compute_checksum(header / data)
    return bytes(header / data)


def valid_packet(pkt):
    if len(pkt) < HEADER_SIZE:
        return False, None, None

    header = PacketHeader(pkt[:HEADER_SIZE])

    if len(pkt) < HEADER_SIZE + header.length:
        return False, None, None

    data = pkt[HEADER_SIZE:HEADER_SIZE + header.length]

    received_checksum = header.checksum
    header.checksum = 0

    if received_checksum != compute_checksum(header / data):
        return False, None, None

    return True, header, data


def sender(receiver_ip, receiver_port, window_size):
    message = sys.stdin.buffer.read()

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver = (receiver_ip, receiver_port)

    # START
    start_packet = make_packet(START, 0)

    while True:
        s.sendto(start_packet, receiver)

        try:
            s.settimeout(TIMEOUT)
            pkt, _ = s.recvfrom(2048)

            ok, header, _ = valid_packet(pkt)

            if ok and header.type == ACK and header.seq_num == 1:
                break

        except socket.timeout:
            pass

    # Tạo DATA packets
    packets = []
    seq_num = 1

    for i in range(0, len(message), MAX_DATA_SIZE):
        chunk = message[i:i + MAX_DATA_SIZE]

        packets.append(
            (seq_num, make_packet(DATA, seq_num, chunk))
        )

        seq_num += 1

    base = 0
    next_packet = 0
    timer_start = None

    s.setblocking(False)

    while base < len(packets):

        # Gửi các packet trong window
        while (
            next_packet < len(packets)
            and next_packet < base + window_size
        ):
            _, pkt = packets[next_packet]
            s.sendto(pkt, receiver)

            if base == next_packet:
                timer_start = time.time()

            next_packet += 1

        try:
            pkt, _ = s.recvfrom(2048)
            ok, header, _ = valid_packet(pkt)

            if not ok or header.type != ACK:
                continue

            # Cumulative ACK
            # ACK N nghĩa là đã nhận tất cả packet < N
            ack_num = header.seq_num

            if base < len(packets) and ack_num > packets[base][0]:

                while (
                    base < len(packets)
                    and packets[base][0] < ack_num
                ):
                    base += 1

                if base < next_packet:
                    timer_start = time.time()
                else:
                    timer_start = None

        except BlockingIOError:
            pass

        # Timeout -> gửi lại toàn bộ packet chưa ACK trong window
        if (
            timer_start is not None
            and time.time() - timer_start >= TIMEOUT
        ):
            for i in range(base, next_packet):
                _, pkt = packets[i]
                s.sendto(pkt, receiver)

            timer_start = time.time()

    s.setblocking(True)

    # END
    end_seq = seq_num
    end_packet = make_packet(END, end_seq)

    s.sendto(end_packet, receiver)

    end_time = time.time()

    while time.time() - end_time < TIMEOUT:
        try:
            remaining = TIMEOUT - (time.time() - end_time)
            s.settimeout(remaining)

            pkt, _ = s.recvfrom(2048)
            ok, header, _ = valid_packet(pkt)

            if (
                ok
                and header.type == ACK
                and header.seq_num == end_seq + 1
            ):
                break

        except socket.timeout:
            break

    s.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)

    args = parser.parse_args()

    sender(
        args.receiver_ip,
        args.receiver_port,
        args.window_size
    )


if __name__ == "__main__":
    main()