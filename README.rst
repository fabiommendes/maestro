=======
Maestro
=======

Maestro is a personal project that helps me in many tasks related to grading and
classroom management. Since it may be useful to other students and educators, I share
the code here in a *AS IS* basis.

I *do not* recommend that you use it as a library of any sort. Unless other people join
to make a community, Maestro will be in a permanent alpha stage and you should expect
major breaks after any version change. If you find some function here useful, I recommend
to copy and paste it into your own project (it is licenced under permissive MIT). If you
want to use in a notebook or as part of your toolset, I recommend to always specify an
exact version number to avoid surprises.


Automated grading based on unit tests
=====================================

First we create a reference implementation. Type annotations helps Maestro
automatically generate valid random inputs.
    

.. code-block:: python
    
    # reference.py
    
    from maestro.types import Int
    
    
    def fib(n: Int[0, 100]):
        """Return a list with the first n Fibonacci numbers"""

        fibs = [1, 1]
        for i in range(n - 2):
            fibs.append(fib[-1] + fib[-2])
        return fibs[:n]


    # This is not necessary if there is a single public function or class
    # defined in the module.
    target = fib


Next we can generate a suite of examples that will be passed to the students::

    $ maestro build

It will create two files: **data.json**, which contains the a set of examples of
input-output pairs and **grade.py**, which is a standalone script that tests 
student code against a subset of the computed examples. 

The **grade.py** file is meant to be given to the students to check their code 
in a test-driven style of development. Once they receive the file, students can
start working by executing:

    $ python3 grade.py init

This will create an stub for students to work on. At any time, students may
grade their work by executing

    $ python3 grade.py grade

This will run a subset of the complete test suite and will provide a temporary 
grade.


Unit tests
----------

Python already has excellent unit test libraries. Maestro doesn't see the need to
reinvent the wheel and instead leverages py.test to define unit tests. When 
writing tests for grading assignments we can use a shortcut: often the
correct answer is known and instead of creating a very comprehensive test suite,
we can simply compare the student's implementation with the reference one.

Maestro provides some test classes and utility functions that helps with that
sittuation by making most of the tests for you.


.. code-block:: python

    from maestro.test import TestFunction, max_grade
    from maestro.types import Int

    # It auto-generates tests from function signature and compares
    class TestCorrectAnswers(TestFunction):
        max_grade = 75
        function = fib

        # We can add additional tests both to test our own implementation and
        # to make sure that the test suite covers some important edge test cases
        def test_negative_values(self, function, reference):
            assert function(-1) == ref(-1)