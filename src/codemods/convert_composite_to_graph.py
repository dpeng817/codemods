from typing import Union, Sequence, Set, Optional, Tuple, cast

import libcst as cst
from libcst.codemod import CodemodContext, VisitorBasedCodemodCommand
from libcst.codemod.visitors import AddImportsVisitor
from libcst.codemod._visitor import ContextAwareTransformer
import libcst.matchers as m
from libcst.codemod.visitors._imports import ImportItem


class ConvertCompositeToGraph(VisitorBasedCodemodCommand):

    # Add a description so that future codemodders can see what this does.
    DESCRIPTION: str = "Converts invocations of composite_solid to graph, renames the function if the function name contains solid, and renames all mention of the former solid's name."

    def __init__(
        self,
        context: CodemodContext,
    ) -> None:
        super().__init__(context)
        self.context.scratch[RenameVariablesVisitor.CONTEXT_KEY] = set()
        self.required_imports: Set[str] = set()

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        if m.matches(
            node,
            m.FunctionDef(
                decorators=(
                    m.ZeroOrMore(),
                    m.Decorator(
                        decorator=m.Name(value="composite_solid")
                        | m.Call(func=m.Name(value="composite_solid")),
                    ),
                    m.ZeroOrMore(),
                )
            ),
        ) and ("solid" in node.name.value or "composite" in node.name.value):
            if not RenameVariablesVisitor.CONTEXT_KEY in self.context.scratch:
                self.context.scratch[RenameVariablesVisitor.CONTEXT_KEY] = set()
            self.context.scratch[RenameVariablesVisitor.CONTEXT_KEY].add(node.name.value)

    def leave_Decorator(
        self, original_node: cst.Decorator, updated_node: cst.Decorator
    ) -> cst.Decorator:
        if m.matches(
            updated_node,
            m.Decorator(decorator=m.Name(value="composite_solid")),
        ):  # bare decorator case
            self.required_imports.add("graph")
            return updated_node.with_changes(
                decorator=updated_node.decorator.with_changes(value="graph")
            )
        if m.matches(
            updated_node,
            m.Decorator(decorator=m.Call(func=m.Name(value="composite_solid"))),
        ):  # case where decorator is invoked a la @composite_solid(...)
            solid_decorator_call = cast(cst.Call, updated_node.decorator)
            solid_args = cast(cst.Call, solid_decorator_call).args
            graph_args = self._convert_solid_args_to_graph_args(solid_args)
            # If an argument was provided that we don't know how to handle, just ignore it.
            if graph_args is None:
                return updated_node
            self.required_imports.add("graph")
            return updated_node.with_changes(
                decorator=solid_decorator_call.with_changes(
                    args=graph_args, func=solid_decorator_call.func.with_changes(value="graph")
                )
            )
        return updated_node

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        # Shenanigans here
        module_solid_refs_removed = self._instantiate_and_run(RenameVariablesVisitor, updated_node)  # type: ignore
        return AddImportsVisitor(
            context=self.context,
            imports=[
                ImportItem("dagster", required_import) for required_import in self.required_imports
            ],
        ).transform_module(module_solid_refs_removed)

    def _convert_solid_args_to_graph_args(
        self, args: Sequence[cst.Arg]
    ) -> Optional[Sequence[cst.Arg]]:
        config_schema_idx = -1
        config_fn_idx = -1
        for i, arg in enumerate(args):
            if m.matches(
                arg, m.Arg(keyword=m.Name(value="input_defs") | m.Name(value="output_defs"))
            ):
                return None
            if m.matches(arg, m.Arg(keyword=m.Name(value="config_fn"))):
                config_fn_idx = i
            if m.matches(arg, m.Arg(keyword=m.Name(value="config_schema"))):
                config_schema_idx = i
        config_mapping_arg = []
        if config_schema_idx != -1 or config_fn_idx != -1:
            self.required_imports.add("ConfigMapping")
            config_mapping_args = []
            if config_schema_idx != -1:
                config_mapping_args.append(args[config_schema_idx])
            if config_fn_idx != -1:
                config_mapping_args.append(args[config_fn_idx])
            config_mapping_arg.append(
                cst.Arg(
                    keyword=cst.Name(value="config"),
                    value=cst.Call(func=cst.Name(value="ConfigMapping"), args=config_mapping_args),
                )
            )
        return [
            *config_mapping_arg,
            *[arg for i, arg in enumerate(args) if i not in [config_fn_idx, config_schema_idx]],
        ]


class RenameVariablesVisitor(ContextAwareTransformer):

    CONTEXT_KEY = "VariableRenames"

    def __init__(self, context: CodemodContext) -> None:
        super().__init__(context)
        self.renames = context.scratch.get(RenameVariablesVisitor.CONTEXT_KEY, set())

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
        if original_node.value in self.renames:
            return original_node.with_changes(value=self._replace(original_node.value))
        return updated_node

    def leave_SimpleString(
        self, original_node: cst.SimpleString, updated_node: cst.SimpleString
    ) -> cst.SimpleString:
        new_val = original_node.value
        for orig_name in self.renames:
            new_name = self._replace(orig_name)
            if orig_name in new_val:
                new_val = new_val.replace(orig_name, new_name)
        return original_node.with_changes(value=new_val)

    def _replace(self, orig_str: str) -> str:
        return (
            orig_str.replace("composite_solid", "graph")
            .replace("composite", "graph")
            .replace("solid", "graph")
        )
