from typing import Union, Sequence, Set, Optional, Tuple, cast

import libcst as cst
from libcst.codemod import CodemodContext, VisitorBasedCodemodCommand
from libcst.codemod.visitors import AddImportsVisitor
from libcst.codemod._visitor import ContextAwareTransformer
import libcst.matchers as m
from libcst.codemod.visitors._imports import ImportItem


class ConvertSolidToOp(VisitorBasedCodemodCommand):

    # Add a description so that future codemodders can see what this does.
    DESCRIPTION: str = "Converts invocations of solid to op, renames the function if the function name contains solid, and renames all mention of the former solid's name."

    def __init__(
        self,
        context: CodemodContext,
    ) -> None:
        super().__init__(context)
        self.context.scratch[RenameVariablesVisitor.CONTEXT_KEY] = set()
        self.required_imports: Set[str] = set()

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        if (
            m.matches(
                node,
                m.FunctionDef(
                    decorators=(
                        m.ZeroOrMore(),
                        m.Decorator(
                            decorator=m.Name(value="solid")
                            | m.Call(func=m.Name(value="solid"))
                            | m.Name(value="lambda_solid")
                            | m.Call(func=m.Name(value="lambda_solid")),
                        ),
                        m.ZeroOrMore(),
                    )
                ),
            )
            and "solid" in node.name.value
        ):
            if not RenameVariablesVisitor.CONTEXT_KEY in self.context.scratch:
                self.context.scratch[RenameVariablesVisitor.CONTEXT_KEY] = set()
            self.context.scratch[RenameVariablesVisitor.CONTEXT_KEY].add(node.name.value)

    def leave_Decorator(
        self, original_node: cst.Decorator, updated_node: cst.Decorator
    ) -> cst.Decorator:
        if m.matches(
            original_node,
            m.Decorator(decorator=m.Name(value="solid") | m.Name(value="lambda_solid")),
        ):  # bare decorator case
            self.required_imports.add("op")
            return original_node.with_changes(
                decorator=original_node.decorator.with_changes(value="op")
            )
        if m.matches(
            original_node,
            m.Decorator(
                decorator=m.Call(func=m.Name(value="solid") | m.Name(value="lambda_solid"))
            ),
        ):  # case where decorator is invoked a la @solid(...)
            solid_decorator_call = cast(cst.Call, original_node.decorator)
            solid_args = cast(cst.Call, solid_decorator_call).args
            op_args = [self._convert_solid_arg_to_op_arg(arg) for arg in solid_args]
            self.required_imports.add("op")
            return original_node.with_changes(
                decorator=solid_decorator_call.with_changes(
                    args=op_args, func=solid_decorator_call.func.with_changes(value="op")
                )
            )
        return updated_node

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        # Shenanigans here
        module_op_body_refs_removed = self._instantiate_and_run(
            RenameVariablesWithinSolidFunctionTransformer, updated_node
        )  # type: ignore
        module_solid_refs_removed = self._instantiate_and_run(RenameVariablesVisitor, module_op_body_refs_removed)  # type: ignore
        return AddImportsVisitor(
            context=self.context,
            imports=[
                ImportItem("dagster", required_import) for required_import in self.required_imports
            ],
        ).transform_module(module_solid_refs_removed)

    def _convert_solid_arg_to_op_arg(self, arg: cst.Arg) -> cst.Arg:
        if m.matches(arg, m.Arg(keyword=m.Name(value="input_defs"), value=m.List())):
            arg_name = cast(cst.Name, arg.keyword)
            input_defs_list = cast(cst.List, arg.value)
            if len(input_defs_list.elements) > 0:
                self.required_imports.add("In")
            ins = _convert_input_defs_to_ins(input_defs_list)
            return arg.with_changes(value=ins, keyword=arg_name.with_changes(value="ins"))
        if m.matches(arg, m.Arg(keyword=m.Name(value="output_defs"), value=m.List())):
            arg_name = cast(cst.Name, arg.keyword)
            output_defs_list = cast(cst.List, arg.value)
            for imprt in _get_required_output_def_imports(output_defs_list):
                self.required_imports.add(imprt)
            outs = _convert_output_defs_to_outs(output_defs_list)
            return arg.with_changes(value=outs, keyword=arg_name.with_changes(value="out"))
        if m.matches(arg, m.Arg(keyword=m.Name(value="output_def"), value=m.Call())):
            self.required_imports.add(_get_import_for_def(cast(cst.Call, arg.value)))
            arg_name = cast(cst.Name, arg.keyword)
            output_def = cast(cst.Call, arg.value)
            new_value = _convert_output_def_to_out(output_def)
            return arg.with_changes(value=new_value, keyword=arg_name.with_changes(value="out"))
        return arg


def _get_required_output_def_imports(output_defs_list: cst.List) -> Set[str]:
    imports = set()

    for element in output_defs_list.elements:
        output_def_call: cst.Call = cast(cst.Call, cast(cst.Element, element).value)
        imports.add(_get_import_for_def(output_def_call))

    return imports


def _get_import_for_def(output_def_call: cst.Call) -> str:
    if cast(cst.Name, output_def_call.func).value == "OutputDefinition":
        return "Out"
    elif cast(cst.Name, output_def_call.func).value == "DynamicOutputDefinition":
        return "DynamicOut"
    else:
        raise Exception(
            f"Unsupported output definition class type '{cast(cst.Name, output_def_call.func).value}'"
        )


def _convert_input_defs_to_ins(input_defs_list: cst.List) -> cst.Dict:
    dict_elements = []
    for element in input_defs_list.elements:
        input_def_call = cast(cst.Element, element).value

        input_def_args = cast(cst.Call, input_def_call).args
        name, rest_of_args = _extract_name_from_input_def_args(input_def_args)
        dict_element = cst.DictElement(
            key=name,
            value=cst.Call(func=cst.Name(value="In"), args=rest_of_args),
        )
        dict_elements.append(dict_element)
    return cst.Dict(elements=dict_elements)


def _convert_output_defs_to_outs(output_defs_list: cst.List) -> Union[cst.Dict, cst.Call]:
    dict_elements = []
    for element in output_defs_list.elements:
        output_def_call: cst.Call = cast(cst.Call, cast(cst.Element, element).value)
        output_def_args = output_def_call.args
        new_type = (
            "Out"
            if cast(cst.Name, output_def_call.func).value == "OutputDefinition"
            else "DynamicOut"
        )
        name, rest_of_args = _extract_name_from_output_def_args(output_def_args)
        # Singleton output case
        if name is None:
            return cst.Call(func=cst.Name(value=new_type), args=rest_of_args)
        dict_element = cst.DictElement(
            key=name,
            value=cst.Call(func=cst.Name(value=new_type), args=rest_of_args),
        )
        dict_elements.append(dict_element)
    return cst.Dict(elements=dict_elements)


def _convert_output_def_to_out(output_def: cst.Call) -> Union[cst.Dict, cst.Call]:
    output_def_args = output_def.args
    new_type = (
        "Out" if cast(cst.Name, output_def.func).value == "OutputDefinition" else "DynamicOut"
    )
    name, rest_of_args = _extract_name_from_output_def_args(output_def_args)
    # Singleton output case
    if name is None:
        return cst.Call(func=cst.Name(value=new_type), args=rest_of_args)
    return cst.Dict(
        elements=[
            cst.DictElement(
                key=name, value=cst.Call(func=cst.Name(value=new_type), args=rest_of_args)
            )
        ]
    )


def _extract_name_from_input_def_args(
    input_def_args: Sequence[cst.Arg],
) -> Tuple[Union[cst.Name, cst.SimpleString], Sequence[cst.Arg]]:
    if input_def_args[0].keyword is None:
        name = cast(Union[cst.Name, cst.SimpleString], input_def_args[0].value)
        rest_of_args = input_def_args[1:]
        return name, rest_of_args

    # Should never be -1 after loop runs
    idx = -1
    for i, input_def_arg in enumerate(input_def_args):
        if input_def_arg.keyword and input_def_arg.keyword.value == "name":
            idx = i

    name = cast(Union[cst.Name, cst.SimpleString], input_def_args[idx].value)
    return name, [input_def_args[i] for i in range(len(input_def_args)) if i != idx]


def _extract_name_from_output_def_args(
    output_def_args: Sequence[cst.Arg],
) -> Tuple[Optional[Union[cst.Name, cst.SimpleString]], Sequence[cst.Arg]]:
    # Attempt to find name by kwarg
    idx = -1
    for i, output_def_arg in enumerate(output_def_args):
        if output_def_arg.keyword and output_def_arg.keyword.value == "name":
            idx = i

    if idx != -1:
        name = cast(Union[cst.Name, cst.SimpleString], output_def_args[idx].value)
        return name, [output_def_args[i] for i in range(len(output_def_args)) if i != idx]
    # Name would be the second positional argument, attempt to find this way
    if len(output_def_args) > 1 and output_def_args[1].keyword is None:
        name = cast(Union[cst.Name, cst.SimpleString], output_def_args[1].value)
        return name, [output_def_arg for i, output_def_arg in enumerate(output_def_args) if i != 1]

    return None, output_def_args


class RenameVariablesWithinSolidFunctionTransformer(ContextAwareTransformer):
    """Renames all variables within solid function."""

    def leave_Attribute(
        self, original_node: cst.Attribute, updated_node: cst.Attribute
    ) -> cst.Attribute:
        node = updated_node
        for orig, replacement in [
            ("solid", "op"),
            ("pipeline_run", "run"),
            ("pipeline_name", "job_name"),
        ]:
            if m.matches(node.value, m.Name(value="context")) and orig in node.attr.value:
                node = node.with_changes(
                    attr=node.attr.with_changes(value=node.attr.value.replace(orig, replacement))
                )
        return node


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
        return orig_str.replace("lambda_solid", "op").replace("solid", "op").replace("lambda", "op")
