"""
LIME UI components. Each module exposes one ``make_*(state)`` factory (or, for
the globe, the ``LakeGlobe`` class). See the CONTRACT in ``GLHE/LIME/app.py``.

Modules are imported directly (``from GLHE.LIME.components.series import
make_series``) rather than re-exported here, so a broken component cannot stop
the others from loading.
"""
