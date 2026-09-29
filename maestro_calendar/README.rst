``maestro.calendar``
====================

Parse calendar files. The format is exemplified bellow::

    Start: 2020-03-09
    End: 2020-07-06
    Weekdays: Mon, Fri
    Skip:
    - 2020-04-10: *Holiday: Paixão de Cristo*
    - 2020-04-20: *Holiday: Tiradentes*

    ----------------------------------------------------------
    First day

    * Can have multiple lines
    * Another sub-topic
    ----------------------------------------------------------
    Second day

    * Each day is separated by a line of at least 3 dashes.
    ----------------------------------------------------------
    And so on...

You can use :func:`maestro.calendar.parse` to obtain a parsed :cls:`maestro.calendar.Calendar` 
object or the auxiliary :func:`maestro.calendar.to_rst` function to render it directly to the
destination format.


Command line
------------

This tool can be used in the command line, type ``maestro.calendar`` or ``python -m maestro.calendar`` in the 
prompt for help.