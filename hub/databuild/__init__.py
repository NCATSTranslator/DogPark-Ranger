"""DogPark-specific build-stage customizations.

biothings' ``BuilderManager.find_builder_classes()`` finds app-level builder
classes by looking at the names defined in *this package* (``import
hub.databuild``), not in its submodules, and it silently ignores an
``ImportError`` from that import. So a builder only becomes usable as a build
config's ``builder_class`` once it is imported here, and anything that breaks
this import makes it disappear, with "No such builder for ..." at merge time
as the only symptom.
"""

from hub.databuild.gandalf_builder import GandalfBuilder  # noqa: F401
