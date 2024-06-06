print("""
I am the __init__.py file in the iisa folder.

I should return an object that is passed network \
subgraph info and returns indexer selections.

""")


class IndexerSelector:
    # TODO better Python 3.7+ type hinting
    """This class is responsible for selecting the indexers for the subgraph."""
    def __init__(self, subgraph_info: dict):
        self.subgraph_info = subgraph_info

    def get_indexer_selections(self):
        return self.subgraph_info
