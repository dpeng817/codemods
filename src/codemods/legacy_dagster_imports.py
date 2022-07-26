import argparse
from ast import literal_eval
from typing import Union, List, Set

import libcst as cst
from libcst.codemod import CodemodContext, VisitorBasedCodemodCommand
from libcst.codemod.visitors import AddImportsVisitor


class LegacyImportCommand(VisitorBasedCodemodCommand):

    # Add a description so that future codemodders can see what this does.
    DESCRIPTION: str = "Converts dagster imports to dagster._legacy imports."

    @staticmethod
    def add_args(arg_parser: argparse.ArgumentParser) -> None:
        # Add command-line args that a user can specify for running this
        # codemod.
        arg_parser.add_argument(
            "--imported",
            dest="symbols",
            metavar="LIST",
            help="Imported symbols to move to `dagster._legacy",
            nargs="+",
            required=True,
        )

    def __init__(self, context: CodemodContext, symbols: List[str]) -> None:
        # Initialize the base class with context, and save our args. Remember, the
        # "dest" for each argument we added above must match a parameter name in
        # this init.
        super().__init__(context)
        self.symbols = set(symbols)

    def leave_ImportFrom(
        self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom
    ) -> cst.ImportFrom:
        if (
            original_node.module
            and original_node.module.value == "dagster"
            and _stmt_imports_symbol(original_node, self.symbols)
        ):
            # Check to see if the string matches what we want to replace. If so,
            # then we do the replacement. We also know at this point that we need
            # to import the constant itself.
            symbols = _get_imported_symbols_to_change(original_node, self.symbols)
            AddImportsVisitor.add_needed_import(
                self.context,
                "dagster._legacy",
                ", ".join(symbols),
            )
            return _get_import_without_symbols(original_node, self.symbols)
        # This isn't an import we're concerned with, so leave it unchanged.
        return updated_node


def _stmt_imports_symbol(node: cst.ImportFrom, symbols: Set[str]) -> bool:
    for name in node.names:
        if name.name.value in symbols:
            return True
    return False


def _get_imported_symbols_to_change(node: cst.ImportFrom, symbols: Set[str]) -> List[str]:
    used_symbols = []
    for import_alias in node.names:
        if import_alias.name.value in symbols:
            used_symbols.append(import_alias.name.value)
    return used_symbols


def _get_import_without_symbols(
    node: cst.ImportFrom, symbols: str
) -> Union[cst.ImportFrom, cst.RemovalSentinel]:
    imports = []
    for i, import_alias in enumerate(node.names):
        if import_alias.name.value not in symbols:
            imports.append(
                # Reconstruct import alias to remove trailing whitespace.
                cst.ImportAlias(
                    name=import_alias.name,
                    asname=import_alias.asname,
                    comma=cst.MaybeSentinel.DEFAULT,
                )
            )
    return (
        cst.ImportFrom(module=node.module, names=imports)
        if len(imports) > 0
        else cst.RemovalSentinel.REMOVE
    )
