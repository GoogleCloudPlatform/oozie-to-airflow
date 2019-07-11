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

from typing import Union, Optional
import re

from lark import Lark, Tree, Token

from .functions import evaluate_function


EL_CONSTANTS = {"KB": "1024", "MB": "1024 ** 2", "GB": "1024 ** 3", "TB": "1024 ** 4", "PB": "1024 ** 5"}

GRAMMAR = r"""
    start: (lvalue (start)?)* | (rvalue (start)?)* | literal_expression (rvalue (start)?)?

    lvalue: BEGIN lvalue_inner END

    rvalue: (BEGIN expression END)+

    lvalue_inner: identifier
        | non_literal_lvalue_prefix (value_suffix)*

    literal_expression: literal_component (/[\$\#]/)?

    literal_component: ((/[^\$\#]/)* (/[\#\$]/)? (/[^\$\#\{]/)+)+
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
        | first_identifier
        | function_invocation

    value_suffix: "." PARAM | "[" expression "]"

    first_identifier: FIRST_JAVA

    identifier: JAVA

    function_invocation: FUNC INVOCATION_COLON FUNC "(" ( expression ( "," expression )* )? ")"
        | FUNC "(" ( expression ( "," expression )* )? ")"

    literal: BOOL | INT | FLOAT | STRING | NULL

    BEGIN: "${" | "#{"

    END: "}"

    INVOCATION_COLON: ":"

    FUNC: /(?!true|false|null)([a-zA-Z_\$][a-zA-Z0-9_\$]*)/

    PARAM: /(?!true|false|null)([a-zA-Z_\$][a-zA-Z0-9_\$]*)/

    JAVA: /(?!true|false|null)([a-zA-Z_\$][a-zA-Z0-9_\$]*)/

    FIRST_JAVA: /(?!true|false|null)([a-zA-Z_\$][a-zA-Z0-9_\$]*)/

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
    return Lark(GRAMMAR, start="start", keep_all_tokens=True, ambiguity="resolve").parse(sentence)


def _camel_to_snake(name: str) -> str:
    """
    Translates CamelCase to snake_.
    """
    sub = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    sub = re.sub("([a-z0-9])([A-Z])", r"\1_\2", sub).lower()
    sub = sub.replace("__", "_")
    return sub


def _translate_binary_operator(tree: Tree) -> str:
    """
    Translates non-python binary operators to python equivalents.
    """
    # Binary op will have only one child and it will be a Token
    operator = tree.children[0]

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

    return str(operator.value)


def _translate_ternary(tree: Tree, functions_module: str) -> Optional[str]:
    """
    Translates ternary expression.
    """
    if tree.data == "expression" and "ternary" in [ch.data for ch in tree.children]:
        # Case of `f() ? true : false`
        condition, ternary = tree.children
        _, if_true, _, if_false = ternary.children
        translation = (
            f"{_translate_el(if_true, functions_module)} if "
            f"{_translate_el(condition, functions_module)} else {_translate_el(if_false, functions_module)}"
        )
        return translation

    if tree.data == "expression1" and "ternary" in [ch.data for ch in tree.children[-1].children]:
        # Case of `x op y ? true : false`
        first = _translate_el(tree.children[0], functions_module)
        operator = _translate_el(tree.children[1], functions_module)

        # expression | ternary
        expression, ternary = tree.children[2].children

        # ? | expression | : | expression
        _, if_true, _, if_false = ternary.children

        second = _translate_el(expression)

        condition = f"{first}{operator}{second}"
        translation = (
            f"{_translate_el(if_true, functions_module)} if "
            f"{condition} else {_translate_el(if_false, functions_module)}"
        )
        return translation

    return None


def _translate_token(token: Token) -> str:
    """
    Translates non-python values to python equivalents.
    """
    if token.type == "BEGIN":
        token.value = " {{"

    if token.type == "END":
        token.value = "}} "

    if token.type == "INVOCATION_COLON":
        token.value = "."

    if token.type == "NULL":
        token.value = None

    if token.type == "FUNC":
        token.value = _camel_to_snake(token.value)

    if token.type == "BOOL":
        if token.value == "true":
            token.value = True
        else:
            token.value = False

    if token.type == "FIRST_JAVA":
        if token.value in EL_CONSTANTS.keys():
            return str(EL_CONSTANTS[token.value])

    return str(token.value)


def _translate_tail(tree: Tree, f_mod: str = "") -> str:
    return "".join([_translate_el(ch, f_mod) for ch in tree.children])


def _unpack_identifier(node: Union[Tree, Token]) -> str:
    if isinstance(node, Token):
        return str(node.value)

    return str(node.children[0].value)


def _get_args(tree: Tree, fun_mod: str) -> tuple:
    args = []

    register = False
    for child in tree.children:
        if isinstance(child, Token) and child.type == "LPAR":
            register = True
            continue

        if isinstance(child, Token) and child.type == "LPAR":
            break

        if isinstance(child, Token) and child.value in (",", ")"):
            continue

        if register:
            args.append(_translate_el(child, fun_mod))

    return tuple(args)


def _translate_function(tree: Tree, f_mod: str) -> str:
    if isinstance(tree.children[1], Token) and tree.children[1].value == ":":
        identifier1 = _unpack_identifier(tree.children[0])
        identifier2 = _unpack_identifier(tree.children[2])

        name = _camel_to_snake(identifier1 + "_" + identifier2)
    else:
        name = _camel_to_snake(tree.children[0].value)

    args = _get_args(tree, f_mod)
    output = evaluate_function(name, args)
    if output:
        return output

    return f_mod + _translate_tail(tree, f_mod)


def _translate_el(tree: Union[Tree, Token], functions_module: str = "") -> str:
    """
    Translates el expression to jinja equivalent.
    """

    if isinstance(tree, Token):
        return _translate_token(tree)

    if tree.data == "binary_op":
        return _translate_binary_operator(tree)

    if tree.data == "function_invocation":
        return _translate_function(tree, functions_module)

    ternary = _translate_ternary(tree, functions_module)
    if ternary is not None:
        return ternary

    return _translate_tail(tree, functions_module)


def _purify(sentence: str) -> str:
    sentence = sentence.strip()
    sentence = re.sub("[ ]+", " ", sentence)
    sentence = sentence.replace("}} / {{", "}}/{{")
    sentence = sentence.replace("}} /", "}}/")
    sentence = sentence.replace("/ {{", "/{{")
    sentence = sentence.replace("}} .", "}}.")
    sentence = sentence.replace(", {{", ",{{")

    return sentence


def translate(expression: str, functions_module: str = "functions", quote: bool = False) -> str:
    """
    Translate Expression Language sentence to Jinja.

    During translation the following transformations are applied:
    - ${, #{   ->  {{
    - name:function   ->  name_function
    - CamelCase -> camel_case

    :param expression: the expression to be translated
    :type expression: str
    :param functions_module: module with python equivalents of el functions
    :type functions_module: str
    :return: translated expression
    :rtype: str
    """
    if functions_module:
        functions_module = functions_module + "."

    ast_tree = _parser(expression)
    translation = _translate_el(ast_tree, functions_module)
    translation = _purify(translation)

    # Here we handle missing or necessary spaces before {{
    if " ${" not in expression and " #{" not in expression:
        translation = translation.replace(" {{", "{{")
    elif " {{" not in translation:
        translation = translation.replace("{{", " {{")

    if quote:
        return "'" + translation + "'"
    return translation
