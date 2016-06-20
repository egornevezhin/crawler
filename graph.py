# pip install networkx

import mysql.connector
import networkx as nx
import matplotlib.pyplot as plt

def connect_mysql():
    config = { 
        'user': 'root', 
        'password': 'root', 
        'host': 'localhost',
        'port': '8889', 
        'database': 'ex',
        }

    db = mysql.connector.connect(**config)       # name of the data base
    return db



def main():
    db = connect_mysql()
    cur = db.cursor()
    cur.execute('SELECT site, parent FROM hlopotov')
    G = nx.Graph()
    dmax = 0
    s = ''
    for site, parent in cur:
        print site
        print parent
        print '*' * 20
        if site not in G.nodes():
            G.add_node(site)
        if parent not in G.nodes():
            G.add_node(parent)
        G.add_edge(site, parent)
    pos=nx.spring_layout(G) 
    nx.draw(G, pos, node_size=[v * 50 for v in G.degree().values()])
    # nx.draw_networkx_labels(G, pos, font_size=8)
    plt.show()
    

if __name__ == "__main__":  

    main()