# -*- coding: utf-8 -*-
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This module contains tools for translating Expression Language to Jinja.

The BNF for the grammar is based on official grammar of EL:
https://download.oracle.com/otn-pub/jcp/jsp-2.1-fr-spec-oth-JSpec/jsp-2_1-fr-spec-el.pdf
"""

__all__ = ["translate"]

from typing import Union
import re

from lark import Lark, Tree, Token


GRAMMAR = r"""
    start: (lvalue (start)?)* | (rvalue (start)?)* | literal_expression (rvalue (start)?)?

    lvalue: "${" lvalue_inner "}" | "#{" lvalue_inner "}"

    rvalue: ("${" expression "}")+ | ("#{" expression "}")+

    lvalue_inner: identifier
        | non_literal_lvalue_prefix (value_suffix)*

    literal_expression: literal_component (/[\$\#]/)?

    // TODO: extend to full rule ! ! !
    literal_component: (/[^\$\#]/)*
        | (/[^\$\#]/)* (/[\$\#][^\{]/)
        | (/[^\$\#]/)* /\\\\/ (/[\$\#]/)?

    expression: expression1 ternary?

    ternary: "?" expression ":" expression

    expression1: expression binary_op expression
        | unary_expression

    binary_op: "and"
        | "&&"
        | "or"
        | "||"
        | "+"
        | "-"
        | "*"
        | "/"
        | "div"
        | "%"
        | "mod"
        | ">"
        | "gt"
        | "<"
        | "lt"
        | ">="
        | "ge"
        | "<="
        | "le"
        | "=="
        | "eq"
        | "!="
        | "ne"

    unary_expression: unary_op unary_expression
        | value

    unary_op: "-"
        | "!"
        | "not"
        | "empty"

    value: value_prefix (value_suffix)*

    value_prefix: literal
        | non_literal_lvalue_prefix

    non_literal_lvalue_prefix: "(" expression ")"
        | identifier
        | function_invocation

    value_suffix: "." identifier | "[" expression "]"

    identifier: JAVA

    function_invocation: (identifier ":")? identifier "(" ( expression ( "," expression )* )? ")"

    literal: BOOL | INT | FLOAT | STRING | NULL

    JAVA: /[a-zA-Z_]+/

    BOOL: "true" | "false"

    STRING: /\'[^\']*\'/ | /\"[^\"]*\"/

    INT: /0|[1-9]\d*/i

    FLOAT: (/[0-9]/)+ "." (/[0-9]/)* EXP?
        | "." (/[0-9]/)+ EXP?
        | (/[0-9]/)+ EXP?

    EXP: /[eE]/ (/[\+\-]/)? (/[0-9]/)+

    NULL: "null"

    %ignore " "
"""


def _parser(sentence: str) -> Tree:
    return Lark(GRAMMAR, start="start", keep_all_tokens=True).parse(sentence)


def _camel_to_snake(name: str) -> str:
    """
    Translates CamelCase to snake_.
    """
    sub = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    sub = re.sub("([a-z0-9])([A-Z])", r"\1_\2", sub).lower()
    sub = sub.replace("__", "_")
    return sub


def _translate_binary_operator(tree: Tree) -> Tree:
    """
    Translates non-python binary operators to python equivalents.
    """
    # Binary op should have only one child and it should be a Token
    assert len(tree.children) == 1

    operator = tree.children[0]
    assert isinstance(operator, Token)

    if operator.value == "gt":
        operator.value = ">"

    if operator.value == "lt":
        operator.value = "<"

    if operator.value == "ge":
        operator.value = ">="

    if operator.value == "le":
        operator.value = "<="

    if operator.value == "ne":
        operator.value = "!="

    if operator.value == "eq":
        operator.value = "=="

    if operator.value == "||":
        operator.value = "or"

    if operator.value == "&&":
        operator.value = "and"

    if operator.value == "mod":
        operator.value = "%"

    if operator.value == "div":
        operator.value = "/"

    operator.value = " " + operator.value + " "

    tree.children = [operator]
    return tree


def _translate_ternary(tree: Tree, case: int) -> str:
    """
    Translates ternary expression.
    """
    if case == 1:
        # f() ? true : false
        condition, ternary = tree.children
        _, if_true, _, if_false = ternary.children
        translation = (
            f"{_translate_el(if_true)} if " f"{_translate_el(condition)} else {_translate_el(if_false)}"
        )
        return translation

    if case == 2:
        # x op y ? true : false
        first = _translate_el(tree.children[0])
        operator = _translate_el(tree.children[1])

        # expression | ternary
        expression, ternary = tree.children[2].children

        # ? | expression | : | expression
        _, if_true, _, if_false = ternary.children

        second = _translate_el(expression)

        condition = f"{first}{operator}{second}"
        translation = f"{_translate_el(if_true)} if {condition} else {_translate_el(if_false)}"
        return translation

    raise Exception("Unhandled ternary expression.")


def _translate_function(tree: Tree) -> Tree:
    """
    Translates function invocations.
    """
    new_children = []
    for child in tree.children:
        if isinstance(child, Token) and child.value == ":":
            child.value = "_"
        new_children.append(child)

    tree.children = new_children
    return tree


def _translate_token(token: Token) -> str:
    """
    Translates non-python values to python equivalents.
    """
    if token.value in ("${", "#{"):
        token.value = "{{"

    if token.value == "}":
        token.value = "}}"

    if token.type == "NULL":
        token.value = None

    if token.type == "BOOL":
        if token.value == "true":
            token.value = True
        token.value = False

    if token.type == "JAVA":
        token.value = _camel_to_snake(token.value)

    return str(token.value)


def _translate_el(tree: Union[Tree, Token]) -> str:
    """
    Translates el expression to jinjia equivalent.
    """
    output = ""

    if isinstance(tree, Token):
        return _translate_token(tree)

    if tree.data == "function_invocation":
        tree = _translate_function(tree)

    if tree.data == "binary_op":
        tree = _translate_binary_operator(tree)

    if tree.data == "expression" and "ternary" in [ch.data for ch in tree.children]:
        return _translate_ternary(tree, case=1)

    if tree.data == "expression1" and "ternary" in [ch.data for ch in tree.children[-1].children]:
        return _translate_ternary(tree, case=2)

    output += "".join([_translate_el(ch) for ch in tree.children])

    return output


def translate(expression: str) -> str:
    """
    Translate Expression Language sentence to Jinja.

    During translation the following transformations are applied:
    - ${, #{   ->  {{
    - name:function   ->  name_function
    - CamelCase -> camel_case

    :param expression: the expression to be translated
    :type expression: str
    :return: translated expression
    :rtype: str
    """

    ast_tree = _parser(expression)
    return _translate_el(ast_tree)
