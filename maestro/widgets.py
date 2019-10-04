"""
Useful widgets to be embedded in a Jupyter notebook.
"""

from IPython.display import display
from hyperpython import iframe, h


def display_youtube(video_id, **kwargs):
    """
    Displays the youtube video with the given video id.

    It accepts all arguments of :func:`show_layout` and :func:`youtube`.
    """

    layout_kwargs, kwargs = split_layout_args(kwargs, ('width', 'height'))
    display_layout(youtube(video_id, **kwargs), **layout_kwargs)


def display_layout(element, **kwargs):
    """
    Show element inside layout.

    See :func:`layout` for more information.
    """
    display(layout(element, **kwargs))


#
# COMPONENTS
#
# Functions in this section return Hyperpython elements instead of displaying
# them using IPython's display function.
#
def layout(element, center=False):
    """
    Layouts the given element according to options.

    Args:
        element:
            An hyperpython element.
        center (bool):
            If True, centralizes the element.
    """
    if center:
        element = h('center', element)
    return element


def youtube(video_id, class_=None, width=640, heigth=480):
    """
    Return an Hyperpython element with an youtube video iframe.

    Args:
        video_id:
            The unique youtube video identifier.
    """
    return iframe(class_=class_, width=width, height=heigth,
                  src='https://www.youtube.com/embed/' + video_id,
                  allow="accelerometer; autoplay; encrypted-media; gyroscope; "
                        "picture-in-picture", allowfullscreen=True, frameborder=0, )


#
# UTILITIES
#
def split_layout_args(kwargs, skip=()):
    """
    Split a dictionary of arguments into arguments that should be applied to
    layout from arguments that should be passed forward.

    Args:
         kwargs (mapping):
            A mapping of names to values
        skip (sequence):
            A optional set of arguments that should not be assigned to layout
            arguments.

    Returns:
         A tuple of (layout_args, non_layout_args) dictionaries.
    """
    layout_args = {'center'}
    layout_args.difference_update(skip)
    in_args = {k: v for k, v in kwargs.items() if k in layout_args}
    out_args = {k: v for k, v in kwargs.items() if k not in layout_args}
    return in_args, out_args
