import json
import networkx as nx
import matplotlib.pyplot as plt


def get_structures(fname):
    with open(fname) as struct_files:
        structures = json.loads(struct_files.read())
    return structures

def get_colors():
    colors = {
        'blood_pressure' : '#57B4E6',
        'blood_glucose' : '#57B4E6',
        'full_blood_count' : '#57B4E6',
        'lipids' : '#57B4E6',
        'liver_function': '#57B4E6',
        'thyroid' : '#57B4E6',
        'urea_and_electrolytes' : '#57B4E6',
        'urin_analysis' : '#57B4E6',
        'root' : '#464F4F'
    }
    for i in xrange(20):
        colors['composition.lbl-%05d' % i] = '#943B3B'
    return colors

def create_graph(graph, parent, structure, colors, child=0):
    try:
        parent_index = parent.split('@')[1]
    except (IndexError, AttributeError):
        parent_index = 0
    if isinstance(structure, dict):
        for node_name, node_children in structure.iteritems():
            node_type = node_name
            if parent is None:
                node_name = 'composition_root'
                node_type = 'root'
            else:
                node_name = "%s@%s.%s" % (node_name, parent_index, child)
            if not node_name.startswith('composition'):
                raise ValueError('Container type %s unknown' % node_name)
            graph.add_node(node_name, color=colors[node_type])
        if parent:
            graph.add_edge(parent, node_name)
        for i, child in enumerate(node_children):
            create_graph(graph, node_name, child, colors, i)
        return graph
    else:
        node_type = structure
        node_name = "%s@%s.%s" % (structure, parent_index, child)
        graph.add_node(node_name, color=colors[node_type])
        if parent:
            graph.add_edge(parent, node_name)
    return graph

def draw_graph(data, label):
    #p=nx.single_source_shortest_path_length(G,ncenter)
    #for i in xrange(0, graphs_num):
    G = nx.DiGraph()
    create_graph(G, None, data, get_colors(), 0)

    node_colors = [G.node[k]['color'] for k in G.nodes()]
    # same layout using matplotlib with no labels
    plt.title("draw_networkx")
    prog = 'dot'

    pos = nx.graphviz_layout(G, prog=prog)
    nx.draw(G, pos=pos, node_size=1000, with_labels=False,
            arrows=False, node_color=node_colors, K=10)
    nx.write_dot(G, 'test%s.dot' % i)
    # plt.show()
    fig = plt.gcf()
    fig.set_size_inches(18.5,10.5)
    plt.savefig('nx_test%s%s.png' % (label, prog))
    plt.clf()


if __name__ == '__main__':
    strs = get_structures('structures_file.json')

    graphs_num = 10
    for i, g in enumerate(strs[:graphs_num]):
        draw_graph(g, i)
    # draw_graph(strs[1], 1)
    # draw_graph(strs[50], 50)