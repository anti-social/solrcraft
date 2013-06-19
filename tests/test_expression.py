from __future__ import unicode_literals

from operator import eq
from unittest import TestCase

from solar.compat import string_types


def eq(a, b):
    return '{}:{}'.format(a, b)


def _literal(element):
    if isinstance(element, Expression):
        return element
    if isinstance(element, string_types):
        return TextExpression(element)
    raise TypeError()
    
class Expression(object):
    def compile(self):
        pass


class BinaryExpression(Expression):
    def __init__(self, op, left, right):
        self.op = op
        self.left = _literal(left)
        self.right = _literal(right)

    def compile(self):
        return self.op(self.left.compile(), self.right.compile())


class TextExpression(Expression):
    def __init__(self, text):
        self.text = text

    def compile(self):
        return self.text
        
class CompareMixin(object):
    def __eq__(self, other):
        return BinaryExpression(eq, self, other)


class Field(Expression, CompareMixin):
    def __init__(self, name):
        self.name = name

    def compile(self):
        return self.name


class ExpressionTest(TestCase):
    def test_expression(self):
        name = Field('name')

        e = name == 'test'
        self.assertEqual(e.compile(), 'name:test')

        # e = name.startswith('test')
        # self.assertEqual(e.compile(), 'name:test*')
