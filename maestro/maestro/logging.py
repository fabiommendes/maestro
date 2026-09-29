from logging import getLogger

from textual.logging import TextualHandler

log = getLogger("maestro")
log.setLevel("DEBUG")
log.addHandler(TextualHandler())
