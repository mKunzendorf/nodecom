from nodecom import udpdds
import unittest
import time
import pickle


class Test_udpdds(unittest.TestCase):
    # def setUp(self):
    #     print("nothing to set up")
    #
    # def tearDown(self):
    #     print("nothing to tear down")

    def test_message_distribution(self):
        debug_setting = False
        with open("test_files/TopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", debug=debug_setting)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", debug=debug_setting)


        self.test_server.start_server()
        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()

        # testing that messages gets distributed to nodes
        self.node_1.send_msg("node_1_status", "node_1_test_message")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()
        self.assertEqual("node_1_test_message", node2_test_msg["message"].decode('utf-8'))
        self.assertEqual("node_1_test_message", node3_test_msg["message"].decode('utf-8'))

        # testing that topic name is not case sensitive to distribution
        self.node_1.send_msg("Node_1_status", "node_1_test_message")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()
        self.assertEqual("node_1_test_message", node2_test_msg["message"].decode('utf-8'))
        self.assertEqual("node_1_test_message", node3_test_msg["message"].decode('utf-8'))

        # testing that messages only gets distributed to correct nodes
        self.node_2.send_msg("Node_2_status", "node_2_test_message")
        time.sleep(0.1)
        node1_test_msg = self.node_1.incoming_msg(blocking=False)
        node2_test_msg = self.node_2.incoming_msg(blocking=False)
        node3_test_msg = self.node_3.incoming_msg(blocking=False)
        self.assertFalse(node1_test_msg["valid"])
        self.assertFalse(node2_test_msg["valid"])
        self.assertEqual("node_2_test_message", node3_test_msg["message"].decode('utf-8'))

        # testing that messages only gets distributed to correct nodes
        self.node_3.send_msg("NoDe_3_status", "node_3_test_message")
        time.sleep(0.1)
        node1_test_msg = self.node_1.incoming_msg(blocking=False)
        node2_test_msg = self.node_2.incoming_msg(blocking=False)
        node3_test_msg = self.node_3.incoming_msg(blocking=False)
        self.assertEqual("node_3_test_message", node1_test_msg["message"].decode('utf-8'))
        self.assertFalse(node2_test_msg["valid"])
        self.assertFalse(node3_test_msg["valid"])

        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()

    def test_too_long_message(self):
        debug_setting = False
        with open("test_files/TopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", debug=debug_setting)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", debug=debug_setting)

        self.test_server.buffer_size = 64
        self.node_1.buffer_size = 64
        self.node_2.buffer_size = 64
        self.node_3.buffer_size = 64

        self.test_server.start_server()
        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()

        self.assertRaises(ValueError, self.node_1.send_msg, "node_1_status", "this message is too long for buffer sizer xxxxxxxxxxxxxxxxxxxxx")

        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()

    def test_pickle(self):
        debug_setting = False
        with open("test_files/TopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", debug=debug_setting)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", debug=debug_setting)

        self.test_server.start_server()
        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()
        # testing sending pickle

        pickle_dict = {"pickle_message": "node_1_test_message"}
        msg = pickle.dumps(pickle_dict)

        self.node_1.send_msg("node_1_status", msg)
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()

        node2_dict = pickle.loads(node2_test_msg["message"])
        node3_dict = pickle.loads(node3_test_msg["message"])

        self.assertEqual("node_1_test_message", node2_dict["pickle_message"])
        self.assertEqual("node_1_test_message", node3_dict["pickle_message"])

        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()

    def test_timestamp(self):
        debug_setting = False
        with open("test_files/TopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", debug=debug_setting)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", debug=debug_setting)
        # testing timestamp
        self.test_server.start_server()
        self.node_1.send_timestamp = True
        self.node_2.send_timestamp = True
        self.node_3.send_timestamp = True
        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()
        self.node_1.send_msg("node_1_status", "node_1_test_message")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()

        if "timestamp" in node2_test_msg:
            node2_test_time = node2_test_msg["recv_timestamp"] - node2_test_msg["timestamp"]
            print("test time: ", node2_test_time)
            if node2_test_time < 5:
                node_2_result = True
            else:
                node_2_result = False
        else:
            node_2_result = False

        if "timestamp" in node3_test_msg:
            node3_test_time = time.time() - node3_test_msg["timestamp"]
            if node3_test_time < 5:
                node_3_result = True
            else:
                node_3_result = False
        else:
            node_3_result = False

        self.assertTrue(node_2_result)
        self.assertTrue(node_3_result)

        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()

    def test_no_timestamp(self):
        debug_setting = False
        with open("test_files/TopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", debug=debug_setting)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", debug=debug_setting)
        # testing timestamp
        self.test_server.start_server()
        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()

        self.node_1.send_msg("node_1_status", "node_1_test_message")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()

        if "timestamp" in node2_test_msg:
            node2_test_time = time.time() - node2_test_msg["timestamp"]
            if node2_test_time < 5:
                node_2_result = True
            else:
                node_2_result = False
        else:
            node_2_result = False

        if "timestamp" in node3_test_msg:
            node3_test_time = time.time() - node3_test_msg["timestamp"]
            if node3_test_time < 5:
                node_3_result = True
            else:
                node_3_result = False
        else:
            node_3_result = False

        self.assertFalse(node_2_result)
        self.assertFalse(node_3_result)
        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()

    def test_send_sender_node(self):
        debug_setting = False
        with open("test_files/TopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", debug=debug_setting)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", debug=debug_setting)
        self.test_server.start_server()
        self.node_1.send_sender_name = True
        self.node_2.send_sender_name = True
        self.node_3.send_sender_name = True
        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()

        self.node_1.send_msg("node_1_status", "node_1_test_message")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()
        self.assertEqual("NODE_1", node2_test_msg["sender"])
        self.assertEqual("NODE_1", node3_test_msg["sender"])
        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()

    def test_long_send_sender_node(self):
        with open("test_files/LongNodeNameTopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="Too_long_Node_nameeeeee", send_sender_name=True, sender_length=25)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", send_sender_name=True, sender_length=25)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", send_sender_name=True, sender_length=25)
        self.test_server.start_server()

        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()

        self.node_1.send_msg("node_1_status", "node_1_test_message")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()
        self.assertEqual("TOO_LONG_NODE_NAMEEEEEE", node2_test_msg["sender"])
        self.assertEqual("TOO_LONG_NODE_NAMEEEEEE", node3_test_msg["sender"])
        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()

    def test_no_sender_node(self):
        debug_setting = False
        with open("test_files/TopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", debug=debug_setting)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", debug=debug_setting)
        self.test_server.start_server()
        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()

        self.node_1.send_msg("node_1_status", "node_1_test_message")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()

        if "sender" in node2_test_msg:
            node_2_result = True
        else:
            node_2_result = False

        if "sender" in node3_test_msg:
            node_3_result = True

        else:
            node_3_result = False

        self.assertFalse(node_2_result)
        self.assertFalse(node_3_result)
        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()

    def test_empty_message(self):
        debug_setting = False
        with open("test_files/TopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", debug=debug_setting)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", debug=debug_setting)
        self.test_server.start_server()
        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()

        self.node_1.send_msg("node_1_status", "")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()

        self.assertEqual("", node2_test_msg["message"].decode('utf-8'))
        self.assertEqual("", node3_test_msg["message"].decode('utf-8'))
        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()

    def test_too_long_topic_in_server(self):
        with open("test_files/CorruptTopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()
        # test that Value Error is raised if too long topic in topic matrix
        self.assertRaises(ValueError, udpdds.Server, topic_matrix)

    def test_too_long_topic_in_client(self):
        debug_setting = False
        with open("test_files/TopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting)

        self.test_server.start_server()
        self.node_1.start_client()

        # test that value error is raised if too long topig is send
        self.assertRaises(ValueError, self.node_1.send_msg, "too_long_topic_length", "node_1_test_message")

    def test_too_long_node_name_client(self):
        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    send_sender_name=True, node_name="Too_long_Node_nameeeeee")

        self.assertRaises(ValueError, self.node_1.start_client)

    def test_long_node_name_client(self):
        # testing that node name leght does not matter when not sending node name
        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="Too_long_Node_nameeeeee")
        self.node_1.start_client()
        self.node_1.close_client()

    def test_long_topic_name(self):
        debug_setting = False
        with open("test_files/CorruptTopicMatrix.csv", "r") as f:
            topic_matrix = f.readlines()
            f.close()

        self.test_server = udpdds.Server(topic_matrix, debug=debug_setting, topic_length=25)

        self.node_1 = udpdds.Client(ip="127.0.0.1", server_recv_port=9998, server_transmit_port=9999,
                                    node_name="NODE_1", debug=debug_setting, topic_length=25)

        self.node_2 = udpdds.Client(ip="127.0.0.1", server_recv_port=9996, server_transmit_port=9997,
                                    node_name="node_2", debug=debug_setting, topic_length=25)

        self.node_3 = udpdds.Client(ip="127.0.0.1", server_recv_port=9994, server_transmit_port=9995,
                                    node_name="Node_3", debug=debug_setting, topic_length=25)

        self.test_server.start_server()
        self.node_1.start_client()
        self.node_2.start_client()
        self.node_3.start_client()

        # testing that messages gets distributed to nodes
        self.node_1.send_msg("too_long_topic_length", "node_1_test_message")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()
        self.assertEqual("node_1_test_message", node2_test_msg["message"].decode('utf-8'))
        self.assertEqual("node_1_test_message", node3_test_msg["message"].decode('utf-8'))

        # testing that topic name is not case sensitive to distribution
        self.node_1.send_msg("Node_1_status", "node_1_test_message")
        node2_test_msg = self.node_2.incoming_msg()
        node3_test_msg = self.node_3.incoming_msg()
        self.assertEqual("node_1_test_message", node2_test_msg["message"].decode('utf-8'))
        self.assertEqual("node_1_test_message", node3_test_msg["message"].decode('utf-8'))

        # testing that messages only gets distributed to correct nodes
        self.node_2.send_msg("Node_2_status", "node_2_test_message")
        time.sleep(0.1)
        node1_test_msg = self.node_1.incoming_msg(blocking=False)
        node2_test_msg = self.node_2.incoming_msg(blocking=False)
        node3_test_msg = self.node_3.incoming_msg(blocking=False)
        self.assertFalse(node1_test_msg["valid"])
        self.assertFalse(node2_test_msg["valid"])
        self.assertEqual("node_2_test_message", node3_test_msg["message"].decode('utf-8'))

        # testing that messages only gets distributed to correct nodes
        self.node_3.send_msg("NoDe_3_status", "node_3_test_message")
        time.sleep(0.1)
        node1_test_msg = self.node_1.incoming_msg(blocking=False)
        node2_test_msg = self.node_2.incoming_msg(blocking=False)
        node3_test_msg = self.node_3.incoming_msg(blocking=False)
        self.assertEqual("node_3_test_message", node1_test_msg["message"].decode('utf-8'))
        self.assertFalse(node2_test_msg["valid"])
        self.assertFalse(node3_test_msg["valid"])

        self.node_1.close_client()
        self.node_2.close_client()
        self.node_3.close_client()
        self.test_server.close_server()


if __name__ == '__main__':
    unittest.main()
