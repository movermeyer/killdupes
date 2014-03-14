import os

from killdupes import killdupes


def test_killdupes():
    path = os.path.dirname(__file__)
    path = os.path.join(path, 'samples')

    pattern = '%s/*' % path
    empty, partial, dupes = killdupes.compute(pattern)

    extract = lambda lst: [os.path.basename(i.name) for i in lst]

    partial = partial.values()[0]
    dupes = dupes.values()[0]

    empty = extract(empty)
    partial = extract(partial)
    dupes = extract(dupes)

    assert empty == ['empty']
    assert sorted(dupes) == ['full', 'full2']
