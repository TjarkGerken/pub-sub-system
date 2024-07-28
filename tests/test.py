import unittest
from unittest.mock import patch, MagicMock

from classes.CommunicationProtocol.SendingCommunicationProtocolSocket import SendingCommunicationProtocolSocket


class TestSendingCommunicationProtocolSocket(unittest.TestCase):

    @patch('classes.CommunicationProtocol.SendingCommunicationProtocolSocket.socket.socket')
    @patch('classes.CommunicationProtocol.SendingCommunicationProtocolSocket.time.time')
    @patch('classes.CommunicationProtocol.SendingCommunicationProtocolSocket.select.select')
    def test_send_message_successful_ack(self, mock_select, mock_time, mock_socket):
        mock_socket_instance = mock_socket.return_value
        mock_time.side_effect = [0, 1, 2, 3, 4, 5]
        mock_select.side_effect = [([], [], []), ([mock_socket_instance], [], [])]
        mock_socket_instance.recvfrom.return_value = (b'ACK', ('127.0.0.1', 5005))

        protocol_socket = SendingCommunicationProtocolSocket('uid', 5001)
        protocol_socket.send = MagicMock()
        protocol_socket.send_message('data', ('127.0.0.1', 5005))

        protocol_socket.send.assert_any_call(('127.0.0.1', 5005), "DATA", 0, 0, 'data')
        protocol_socket.send.assert_any_call(('127.0.0.1', 5005), "ACK", 0, 2, "ACK")

    @patch('classes.CommunicationProtocol.SendingCommunicationProtocolSocket.socket.socket')
    @patch('classes.CommunicationProtocol.SendingCommunicationProtocolSocket.time.time')
    @patch('classes.CommunicationProtocol.SendingCommunicationProtocolSocket.select.select')
    def test_send_message_retries_exhausted(self, mock_select, mock_time, mock_socket):
        mock_socket_instance = mock_socket.return_value
        mock_time.side_effect = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        mock_select.return_value = ([], [], [])

        protocol_socket = SendingCommunicationProtocolSocket('uid', 5001)
        protocol_socket.send = MagicMock()
        protocol_socket.send_message('data', ('127.0.0.1', 5005))

        protocol_socket.send.assert_called_with(('127.0.0.1', 5005), "DATA", 0, 0, 'data')
        self.assertEqual(protocol_socket.sequence_number, 1)

    @patch('classes.CommunicationProtocol.SendingCommunicationProtocolSocket.socket.socket')
    @patch('classes.CommunicationProtocol.SendingCommunicationProtocolSocket.time.time')
    @patch('classes.CommunicationProtocol.SendingCommunicationProtocolSocket.select.select')
    def test_send_message_ack_not_received(self, mock_select, mock_time, mock_socket):
        mock_socket_instance = mock_socket.return_value
        mock_time.side_effect = [0, 1, 2, 3, 4, 5]
        mock_select.side_effect = [([], [], []), ([], [], [])]
        mock_socket_instance.recvfrom.return_value = (b'', ('127.0.0.1', 5005))

        protocol_socket = SendingCommunicationProtocolSocket('uid', 5001)
        protocol_socket.send = MagicMock()
        protocol_socket.send_message('data', ('127.0.0.1', 5005))

        protocol_socket.send.assert_called_with(('127.0.0.1', 5005), "DATA", 0, 0, 'data')
        self.assertEqual(protocol_socket.sequence_number, 1)


if __name__ == '__main__':
    unittest.main()
