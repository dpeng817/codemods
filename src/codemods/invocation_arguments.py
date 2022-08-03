import argparse
from ast import literal_eval
from typing import Union, Sequence, Set, Optional

import libcst as cst
from libcst.codemod import CodemodContext, VisitorBasedCodemodCommand
from libcst.codemod.visitors import AddImportsVisitor


class SwitchInvocationArgumentsCommand(VisitorBasedCodemodCommand):

    # Add a description so that future codemodders can see what this does.
    DESCRIPTION: str = (
        "Switches the arguments of a function/class invocation to the ones specified."
    )

    @staticmethod
    def add_args(arg_parser: argparse.ArgumentParser) -> None:
        # Add command-line args that a user can specify for running this
        # codemod.
        arg_parser.add_argument(
            "--symbol",
            dest="symbol",
            metavar="STRING",
            help="Function/class being invoked",
            required=True,
        )
        arg_parser.add_argument(
            "--orig_arg",
            dest="original_arg",
            metavar="STRING",
            help="Original argument to change",
            required=True,
        )
        arg_parser.add_argument(
            "--orig_arg_pos",
            dest="original_arg_position",
            metavar="INT",
            help="Position of original argument",
            required=True,
        )
        arg_parser.add_argument(
            "--replace_arg",
            dest="replacement_arg",
            metavar="STRING",
            help="Argument to change to.",
            required=False,
        )

    def __init__(
        self,
        context: CodemodContext,
        symbol: str,
        original_arg: str,
        original_arg_position: int,
        replacement_arg: Optional[str],
    ) -> None:
        # Initialize the base class with context, and save our args. Remember, the
        # "dest" for each argument we added above must match a parameter name in
        # this init.
        super().__init__(context)
        self.symbol = symbol
        self.original_arg = original_arg
        self.original_arg_position = int(original_arg_position)
        self.replacement_arg = replacement_arg

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        if isinstance(original_node.func, cst.Name) and original_node.func.value == self.symbol:
            return self._replace_arg_in_invocation(original_node)
        # This isn't an invocation we're concerned with, so leave it unchanged.
        return updated_node

    def _replace_arg_in_invocation(self, node: cst.Call) -> cst.Call:
        arg_position = self._get_arg_pos(node.args)
        if arg_position is None:
            return node
        if self.replacement_arg:
            arg = node.args[arg_position]
            old_keyword = arg.keyword
            new_keyword = cst.Name(
                value=self.replacement_arg, lpar=old_keyword.lpar, rpar=old_keyword.rpar
            )
            new_arg = cst.Arg(
                value=arg.value,
                keyword=new_keyword,
                equal=arg.equal,
                comma=arg.comma,
                star=arg.star,
                whitespace_after_star=arg.whitespace_after_star,
                whitespace_after_arg=arg.whitespace_after_arg,
            )
            new_arg_list = [
                node.args[i] if i != arg_position else new_arg for i in range(len(node.args))
            ]
        else:
            new_arg_list = [node.args[i] for i in range(len(node.args)) if i != arg_position]
        return cst.Call(
            func=node.func,
            args=new_arg_list,
            lpar=node.lpar,
            rpar=node.rpar,
            whitespace_after_func=node.whitespace_after_func,
            whitespace_before_args=node.whitespace_before_args,
        )

    def _get_arg_pos(self, arg_list: Sequence[cst.Arg]) -> Optional[int]:
        for i, arg in enumerate(arg_list):
            if arg.keyword and arg.keyword.value == self.original_arg:
                return i
        if (
            len(arg_list) > self.original_arg_position
            and arg_list[self.original_arg_position].keyword is None
        ):
            return self.original_arg_position
        return None
