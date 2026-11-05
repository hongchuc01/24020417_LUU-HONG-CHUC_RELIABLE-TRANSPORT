import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

START = 0
END = 1
DATA = 2
ACK = 3

HEADER_SIZE = 16


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


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    connected = False
    sender_address = None
    expected_seq = 1

    # Buffer packet đến sai thứ tự
    buffer = {}

    while True:
        pkt, address = s.recvfrom(2048)

        ok, header, data = valid_packet(pkt)

        # Checksum lỗi -> drop, không ACK
        if not ok:
            continue

        # START
        if header.type == START:

            if not connected:
                connected = True
                sender_address = address
                expected_seq = 1
                buffer = {}

                s.sendto(
                    make_packet(ACK, 1),
                    sender_address
                )

            continue

        if not connected or address != sender_address:
            continue

        # DATA
        if header.type == DATA:
            seq = header.seq_num

            # Ngoài receive window
            if seq >= expected_seq + window_size:
                s.sendto(
                    make_packet(ACK, expected_seq),
                    sender_address
                )
                continue

            # Packet cũ/trùng
            if seq < expected_seq:
                s.sendto(
                    make_packet(ACK, expected_seq),
                    sender_address
                )
                continue

            # Buffer packet
            if seq not in buffer:
                buffer[seq] = data

            # Xuất các packet liên tiếp
            while expected_seq in buffer:
                sys.stdout.buffer.write(buffer[expected_seq])
                del buffer[expected_seq]
                expected_seq += 1

            sys.stdout.buffer.flush()

            # Cumulative ACK
            s.sendto(
                make_packet(ACK, expected_seq),
                sender_address
            )

        # END
        elif header.type == END:

            if header.seq_num == expected_seq:

                s.sendto(
                    make_packet(ACK, expected_seq + 1),
                    sender_address
                )

                break

            else:
                s.sendto(
                    make_packet(ACK, expected_seq),
                    sender_address
                )

    s.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)

    args = parser.parse_args()

    receiver(
        args.receiver_ip,
        args.receiver_port,
        args.window_size
    )


if __name__ == "__main__":
    main()