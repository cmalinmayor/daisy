from __future__ import absolute_import

import daisy
import logging
import unittest
import time
import random
import sys

logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.DEBUG)
daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


class TestFilterMongoGraph(unittest.TestCase):

    def get_mongo_graph_provider(self, mode, node_attributes, edge_attributes):
        return daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            directed=True,
            node_attribute_collections=node_attributes,
            edge_attribute_collections=edge_attributes,
            mode=mode)

    def test_graph_separate_collection_simple(self):
        attributes = {'1': ['selected']}
        graph_provider = self.get_mongo_graph_provider(
                'w', attributes, attributes)
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

        graph_provider = self.get_mongo_graph_provider(
                'r', attributes, attributes)
        compare_graph = graph_provider[roi]

        self.assertEqual(True, compare_graph.nodes[2]['selected'])
        self.assertEqual(False, compare_graph.nodes[42]['selected'])
        self.assertEqual(True, compare_graph.edges[2, 42]['selected'])
        self.assertEqual(False, compare_graph.edges[42, 23]['selected'])

    def test_graph_separate_collection_missing_attrs(self):
        attributes = {'2': ['selected']}
        graph_provider = self.get_mongo_graph_provider(
                'w', attributes, attributes)
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        graph = graph_provider[roi]

        graph.add_node(2, position=(2, 2, 2))
        graph.add_node(42, position=(1, 1, 1), selected=False)
        graph.add_node(23, position=(5, 5, 5), selected=True)
        graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), selected=True)
        graph.add_edge(42, 23)
        graph.add_edge(57, 23, selected=True)
        graph.add_edge(2, 42, selected=True)

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider(
                'r', attributes, attributes)
        compare_graph = graph_provider[roi]

        self.assertFalse('selected' in compare_graph.nodes[2])
        self.assertEqual(False, compare_graph.nodes[42]['selected'])
        self.assertEqual(True, compare_graph.edges[2, 42]['selected'])
        self.assertFalse('selected' in compare_graph.edges[42, 23])

    def test_graph_multiple_separate_collections(self):
        attributes = {'3': ['selected'], '4': ['swip']}
        graph_provider = self.get_mongo_graph_provider(
                'w', attributes, attributes)
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        graph = graph_provider[roi]

        graph.add_node(2, position=(2, 2, 2), swip='swap')
        graph.add_node(42, position=(1, 1, 1), selected=False, swip='swim')
        graph.add_node(23, position=(5, 5, 5), selected=True)
        graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), selected=True)
        graph.add_edge(42, 23)
        graph.add_edge(57, 23, selected=True, swip='swap')
        graph.add_edge(2, 42, selected=True)
        graph.add_edge(42, 2, swip='swim')

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider(
                'r', attributes, attributes)
        compare_graph = graph_provider[roi]

        self.assertFalse('selected' in compare_graph.nodes[2])
        self.assertEqual('swap', compare_graph.nodes[2]['swip'])
        self.assertEqual(False, compare_graph.nodes[42]['selected'])
        self.assertEqual('swim', compare_graph.nodes[42]['swip'])
        self.assertFalse('swip' in compare_graph.nodes[57])
        self.assertEqual(True, compare_graph.edges[2, 42]['selected'])
        self.assertFalse('swip' in compare_graph.edges[2, 42])
        self.assertFalse('selected' in compare_graph.edges[42, 23])
        self.assertEqual('swim', compare_graph.edges[42, 2]['swip'])

    def test_graph_multiple_attrs_per_collection(self):
        attributes = {'5': ['selected', 'swip']}
        graph_provider = self.get_mongo_graph_provider(
                'w', attributes, attributes)
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        graph = graph_provider[roi]

        graph.add_node(2, position=(2, 2, 2), swip='swap')
        graph.add_node(42, position=(1, 1, 1), selected=False, swip='swim')
        graph.add_node(23, position=(5, 5, 5), selected=True)
        graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), selected=True)
        graph.add_edge(42, 23)
        graph.add_edge(57, 23, selected=True, swip='swap')
        graph.add_edge(2, 42, selected=True)
        graph.add_edge(42, 2, swip='swim')

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider(
                'r', attributes, attributes)
        compare_graph = graph_provider[roi]

        self.assertFalse('selected' in compare_graph.nodes[2])
        self.assertEqual('swap', compare_graph.nodes[2]['swip'])
        self.assertEqual(False, compare_graph.nodes[42]['selected'])
        self.assertEqual('swim', compare_graph.nodes[42]['swip'])
        self.assertFalse('swip' in compare_graph.nodes[57])
        self.assertEqual(True, compare_graph.edges[2, 42]['selected'])
        self.assertFalse('swip' in compare_graph.edges[2, 42])
        self.assertFalse('selected' in compare_graph.edges[42, 23])
        self.assertEqual('swim', compare_graph.edges[42, 2]['swip'])

    def get_random_nodes(self, num_nodes, attrs, roi):
        offset = roi.get_offset()
        o1, o2, o3 = offset
        shape = roi.get_shape()
        s1, s2, s3 = shape
        return [
                (n, dict(
                    {'position': [random.randrange(offset, offset + shape)
                                  for offset, shape in zip(
                                      roi.get_offset(),
                                      roi.get_shape())]},
                    **{attr: random.choice([True, False])
                       for attr in attrs}))
                for n in range(num_nodes)
            ]

    def get_random_edges(self, num_nodes, num_edges, attrs):
        return [
                (random.randrange(0, num_nodes),
                 random.randrange(0, num_nodes),
                 {attr: random.choice([True, False])
                     for attr in attrs})
                for n in range(num_edges)
            ]

    def benchmark_graph_separate_collections(self):
        num_attrs = 4
        attrs = ['attr_' + str(a) for a in range(num_attrs)]
        powers_10 = 5
        edges_per_node = 2

        write_times = {}
        read_times = {}
        for power in range(1, powers_10):
            num_nodes = 10 ** power
            num_edges = num_nodes * edges_per_node
            roi = daisy.Roi((0, 0, 0),
                            (num_nodes, num_nodes, num_nodes))
            nodes = self.get_random_nodes(num_nodes, attrs, roi)
            edges = self.get_random_edges(num_nodes, num_edges, attrs)
            write_times_by_num_separate = {}
            read_times_by_num_separate = {}
            for num_separate in range(num_attrs):
                separate_attrs = {attr: [attr]
                                  for attr in attrs[:num_separate]}
                write_provider = self.get_mongo_graph_provider(
                        'w', separate_attrs, separate_attrs)
                write_graph = write_provider[roi]
                write_graph.add_nodes_from(nodes)
                write_graph.add_edges_from(edges)
                write_start_time = time.time()
                write_graph.write_nodes()
                write_graph.write_edges()
                write_time = time.time() - write_start_time
                write_times_by_num_separate[num_separate] = write_time

                read_provider = self.get_mongo_graph_provider(
                        'r', separate_attrs, separate_attrs)
                read_start_time = time.time()
                read_provider[roi]
                read_time = time.time() - read_start_time
                read_times_by_num_separate[num_separate] = read_time
            write_times[num_nodes] = write_times_by_num_separate
            read_times[num_nodes] = read_times_by_num_separate

        self.print_times('write', write_times)
        self.print_times('read', read_times)

    def print_times(self, title, times):
        print('Benchmark %s times:' % title)
        x_axis_values = None
        for num_nodes, line_values in times.items():
            if x_axis_values is None:
                x_axis_values = line_values.keys()
                x_axis_strings = [str(v) for v in x_axis_values]
                print('\t%s' % '\t'.join(x_axis_strings))
            values = ['%.3f' % line_values[key] for key in x_axis_values]
            line = str(num_nodes) + '\t' + '\t'.join(values)
            print(line)


if __name__ == '__main__':
    if sys.argv[1] == 'benchmark':
        test = TestFilterMongoGraph()
        test.benchmark_graph_separate_collections()
