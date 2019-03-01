from __future__ import absolute_import

import daisy
import logging
import unittest

logger = logging.getLogger(__name__)
daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


class TestFilterMongoGraph(unittest.TestCase):

    def get_mongo_graph_provider(self, mode):
        return daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            directed=True,
            mode=mode)

    def test_graph_filtering(self):
        graph_provider = self.get_mongo_graph_provider('w')
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        graph = graph_provider[roi]

        graph.add_node(2, position=(2, 2, 2), selected=True)
        graph.add_node(42, position=(1, 1, 1), selected=False)
        graph.add_node(23, position=(5, 5, 5), selected=True)
        graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), selected=True)
        graph.add_edge(42, 23, selected=False)
        graph.add_edge(57, 23, selected=True)
        graph.add_edge(2, 42, selected=True)

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider('r')

        filtered_nodes = graph_provider.read_filtered_nodes(
                roi, 'selected', True)
        filtered_node_ids = [node['id'] for node in filtered_nodes]
        expected_node_ids = [2, 23, 57]
        self.assertCountEqual(expected_node_ids, filtered_node_ids)

        filtered_edges = graph_provider.read_filtered_edges(
                roi, 'selected', True)
        filtered_edge_endpoints = [(edge['u'], edge['v'])
                                   for edge in filtered_edges]
        expected_edge_endpoints = [(57, 23), (2, 42)]
        self.assertCountEqual(expected_edge_endpoints, filtered_edge_endpoints)

    def test_graph_filtering_separate(self):
        graph_provider = self.get_mongo_graph_provider('w')
        graph_provider.prepare_node_attribute_collection('selected',
                                                         clear=True)
        graph_provider.prepare_edge_attribute_collection('selected',
                                                         clear=True)
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        graph = graph_provider[roi]

        graph.add_node(2, position=(2, 2, 2), selected=True)
        graph.add_node(42, position=(1, 1, 1), selected=False)
        graph.add_node(23, position=(5, 5, 5), selected=True)
        graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), selected=True)
        graph.add_edge(42, 23, selected=False)
        graph.add_edge(57, 23, selected=True)
        graph.add_edge(2, 42, selected=True)

        graph.write_nodes(attributes=['position'])
        graph.write_edges(attributes=[])
        graph.write_nodes_attribute_in_collection('selected')
        graph.write_edge_attribute_in_collection('selected')

        graph_provider = self.get_mongo_graph_provider('r')

        filtered_nodes = graph_provider.read_filtered_nodes(
                roi, 'selected', True, separate=True)
        filtered_node_ids = [node['id'] for node in filtered_nodes]
        expected_node_ids = [2, 23, 57]
        self.assertCountEqual(expected_node_ids, filtered_node_ids)

        filtered_edges = graph_provider.read_filtered_edges(
                roi, 'selected', True, separate=True)
        filtered_edge_endpoints = [(edge['u'], edge['v'])
                                   for edge in filtered_edges]
        expected_edge_endpoints = [(57, 23), (2, 42)]
        self.assertCountEqual(expected_edge_endpoints, filtered_edge_endpoints)

        node_filtered_subgraph = graph_provider.get_filtered_subgraph(
                roi, node_attr='selected', node_value=True, node_separate=True)
        filtered_nodes = node_filtered_subgraph.nodes(data=True)
        filtered_nodes = [node for node, data in filtered_nodes
                          if 'position' in data]
        self.assertCountEqual(expected_node_ids, filtered_nodes)
        # Expect only edges with sources in the filtered nodes
        self.assertCountEqual(expected_edge_endpoints,
                              node_filtered_subgraph.edges())

        edge_filtered_subgraph = graph_provider.get_filtered_subgraph(
                roi, edge_attr='selected', edge_value=True, edge_separate=True)
        self.assertCountEqual(expected_edge_endpoints,
                              edge_filtered_subgraph.edges())
