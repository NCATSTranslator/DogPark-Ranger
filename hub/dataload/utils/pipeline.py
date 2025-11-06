from functools import reduce
from typing import Callable


def apply_processors(processors: list[Callable], doc):
    return reduce(lambda acc, func: func(acc), processors, doc)



# sample processer
def id_processor(doc):
    return doc
