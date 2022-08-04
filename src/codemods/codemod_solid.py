import argparse
from ast import literal_eval
from typing import Union, Sequence, Set, Optional, Tuple, cast, Dict

import libcst as cst
from libcst.codemod import CodemodContext, VisitorBasedCodemodCommand
from libcst.codemod.visitors import AddImportsVisitor
from libcst.codemod._visitor import ContextAwareTransformer


class CodemodSolid(VisitorBasedCodemodCommand):

    # Add a description so that future codemodders can see what this does.
    DESCRIPTION: str = (
        "Switches the arguments of a function/class invocation to the ones specified."
    )

    @staticmethod
    def add_args(arg_parser: argparse.ArgumentParser) -> None:
        # Add command-line args that a user can specify for running this
        # codemod.
        pass

    def __init__(
        self,
        context: CodemodContext,
    ) -> None:
        # Initialize the base class with context, and save our args. Remember, the
        # "dest" for each argument we added above must match a parameter name in
        # this init.
        super().__init__(context)

    def transform_module(self, tree: cst.Module) -> cst.Module:
        tree = super().transform_module(tree)

        supported_transforms: Dict[str, Type[Codemod]] = {
            RenameVariablesVisitor.CONTEXT_KEY: RenameVariablesVisitor,
        }

        # For any visitors that we support auto-running, run them here if needed.
        for key, transform in supported_transforms.items():
            if key in self.context.scratch:
                # We have work to do, so lets run this.
                tree = self._instantiate_and_run(transform, tree)

        # We're finally done!
        return tree

    def leave_FunctionDef(
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef:
        if (
            len(original_node.decorators) > 0
            and _get_solid_decorator_pos(original_node.decorators) != -1
        ):
            return self.replace_solid(original_node)
        # This isn't a function def we're concerned with, so leave it unchanged.
        return updated_node

    def replace_solid(self, node: cst.FunctionDef) -> cst.FunctionDef:
        solid_def = node
        new_name = solid_def.name.value.replace("solid", "op")
        RenameVariablesVisitor.rename_variable(self.context, solid_def.name.value, new_name)
        solid_decorator_pos = _get_solid_decorator_pos(solid_def.decorators)
        replaced_decorator = self._replace_decorator(solid_def.decorators[solid_decorator_pos])
        new_decorator_list = [
            decorator if i != solid_decorator_pos else replaced_decorator
            for i, decorator in enumerate(solid_def.decorators)
        ]

        return solid_def.with_changes(
            name=solid_def.name.with_changes(value=new_name),
            decorators=new_decorator_list,
            body=self._instantiate_and_run(
                RenameVariablesWithinSolidFunctionVisitor, solid_def.body
            ),
        )

    def _replace_decorator(self, decorator: cst.Decorator) -> cst.Decorator:
        AddImportsVisitor.add_needed_import(
            self.context,
            "dagster",
            "op",
        )
        if isinstance(decorator.decorator, cst.Name):
            return decorator.with_changes(decorator=decorator.decorator.with_changes(value="op"))
        else:
            call_node = cast(cst.Call, decorator.decorator)
            new_call = self._replace_args(call_node)
            return decorator.with_changes(decorator=new_call)

    def _replace_args(self, node: cst.Call) -> cst.Call:
        return self._replace_arg(self._replace_arg(node, "input"), "output")

    def _replace_arg(self, node: cst.Call, io_type: str) -> cst.Call:
        if io_type == "input":
            keyword = "ins"
            original_arg_name = "input_defs"
            _convert_fn = self._convert_input_defs_to_ins
        else:
            keyword = "out"
            original_arg_name = "output_defs"
            _convert_fn = self._convert_output_defs_to_out

        pos = self._get_arg_pos(node.args, original_arg_name)
        if pos is None:
            return node.with_changes(func=node.func.with_changes(value="op"))
        arg = node.args[pos]
        new_arg = cst.Arg(keyword=cst.Name(value=keyword), value=_convert_fn(arg.value))

        new_arg_list = [node.args[i] if i != pos else new_arg for i in range(len(node.args))]

        return node.with_changes(func=node.func.with_changes(value="op"), args=new_arg_list)

    def _get_arg_pos(self, arg_list: Sequence[cst.Arg], keyword: str) -> Optional[int]:
        for i, arg in enumerate(arg_list):
            if arg.keyword and arg.keyword.value == keyword:
                return i
        return None

    def _convert_input_defs_to_ins(self, input_defs_list: cst.List) -> cst.Dict:
        dict_elements = []
        for input_def_call in input_defs_list.elements:
            input_def_args = input_def_call.value.args
            name, rest_of_args = self._extract_name_from_input_def_args(input_def_args)
            dict_element = cst.DictElement(
                key=cst.SimpleString(value=name),
                value=cst.Call(func=cst.Name(value="In"), args=rest_of_args),
            )
            dict_elements.append(dict_element)
            AddImportsVisitor.add_needed_import(
                self.context,
                "dagster",
                "In",
            )
        return cst.Dict(elements=dict_elements)

    def _convert_output_defs_to_out(self, output_defs_list: cst.List) -> cst.Dict:
        dict_elements = []
        for output_def_call in output_defs_list.elements:
            output_def_args = output_def_call.value.args
            new_type = (
                "Out" if output_def_call.value.func.value == "OutputDefinition" else "DynamicOut"
            )
            AddImportsVisitor.add_needed_import(
                self.context,
                "dagster",
                new_type,
            )
            name, rest_of_args = self._extract_name_from_output_def_args(output_def_args)
            # Singleton output case
            if name is None:
                return cst.Call(func=cst.Name(value=new_type), args=rest_of_args)
            dict_element = cst.DictElement(
                key=cst.SimpleString(value=name),
                value=cst.Call(func=cst.Name(value=new_type), args=rest_of_args),
            )
            dict_elements.append(dict_element)
        return cst.Dict(elements=dict_elements)

    def _extract_name_from_input_def_args(
        self, input_def_args: Sequence[cst.Arg]
    ) -> Tuple[str, Sequence[cst.Arg]]:
        if input_def_args[0].keyword is None:
            name = input_def_args[0].value.value
            rest_of_args = input_def_args[1:]
            return name, rest_of_args

        # Should never be -1 after loop runs
        idx = -1
        for i, input_def_arg in enumerate(input_def_args):
            if input_def_arg.keyword and input_def_arg.keyword.value == "name":
                idx = i

        return input_def_args[idx].value.value, [
            input_def_args[i] for i in range(len(input_def_args)) if i != idx
        ]

    def _extract_name_from_output_def_args(
        self, output_def_args: Sequence[cst.Arg]
    ) -> Tuple[str, Sequence[cst.Arg]]:
        # Attempt to find name by kwarg
        idx = -1
        for i, output_def_arg in enumerate(output_def_args):
            if output_def_arg.keyword and output_def_arg.keyword.value == "name":
                idx = i

        if idx != -1:
            return output_def_args[idx].value.value, [
                output_def_args[i] for i in range(len(output_def_args)) if i != idx
            ]
        # Name would be the second positional argument, attempt to find this way
        if len(output_def_args) > 1 and isinstance(output_def_args[1].value, cst.SimpleString):
            return output_def_args[1].value.value, [
                output_def_arg for i, output_def_arg in enumerate(output_def_args) if i != 1
            ]

        return None, output_def_args


def _get_solid_decorator_pos(decorator_seq: Sequence[cst.Decorator]) -> bool:
    for i, decorator in enumerate(decorator_seq):
        if isinstance(decorator.decorator, cst.Name) and decorator.decorator.value == "solid":
            return i
        if isinstance(decorator.decorator, cst.Call) and decorator.decorator.func.value == "solid":
            return i

    return -1


class RenameVariablesVisitor(ContextAwareTransformer):

    CONTEXT_KEY = "RenameVariablesVisitor"

    @staticmethod
    def rename_variable(context: CodemodContext, orig_var_name: str, new_var_name: str) -> None:
        renames = context.scratch.get(RenameVariablesVisitor.CONTEXT_KEY, {})
        renames[orig_var_name] = new_var_name
        context.scratch[RenameVariablesVisitor.CONTEXT_KEY] = renames

    def __init__(self, context: CodemodContext) -> None:
        super().__init__(context)
        self.renames = context.scratch[RenameVariablesVisitor.CONTEXT_KEY]

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
        if original_node.value in self.renames:
            return cst.Name(
                value=self.renames[original_node.value],
                lpar=original_node.lpar,
                rpar=original_node.rpar,
            )
        return updated_node


class RenameVariablesWithinSolidFunctionVisitor(ContextAwareTransformer):
    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
        if "solid" in original_node.value:
            return original_node.with_changes(value=original_node.value.replace("solid", "op"))
        if "pipeline_run" in original_node.value:
            return original_node.with_changes(
                value=original_node.value.replace("pipeline_run", "run")
            )
        if "pipeline_name" in original_node.value:
            return original_node.with_changes(
                value=original_node.value.replace("pipeline_name", "job_name")
            )
        return updated_node
