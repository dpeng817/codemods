from typing import Union, Sequence, Set, Optional, Tuple, cast, Dict

import libcst as cst
from libcst.codemod import CodemodContext, VisitorBasedCodemodCommand
from libcst.codemod.visitors import AddImportsVisitor
from libcst.codemod._visitor import ContextAwareTransformer
import libcst.matchers as m
from libcst.codemod.visitors._imports import ImportItem


class ConvertPipelineToJob(VisitorBasedCodemodCommand):

    # Add a description so that future codemodders can see what this does.
    DESCRIPTION: str = "Converts invocations of pipeline to job, renames the function if the function name contains job, and renames all mention of the former job's name."

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
                            decorator=m.Name(value="pipeline")
                            | m.Call(func=m.Name(value="pipeline"))
                        ),
                        m.ZeroOrMore(),
                    )
                ),
            )
            and "pipeline" in node.name.value
        ):
            if not RenameVariablesVisitor.CONTEXT_KEY in self.context.scratch:
                self.context.scratch[RenameVariablesVisitor.CONTEXT_KEY] = set()
            self.context.scratch[RenameVariablesVisitor.CONTEXT_KEY].add(node.name.value)

    def leave_FunctionDef(
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef:
        if m.matches(
            updated_node,
            m.FunctionDef(decorators=(m.Decorator(decorator=m.Name(value="pipeline")),)),
        ):  # bare decorator case
            pipeline_decorator = updated_node.decorators[0]
            self.required_imports.add("job")
            return updated_node.with_deep_changes(
                cast(cst.Name, pipeline_decorator.decorator), value="job"
            )
        elif m.matches(
            updated_node,
            m.FunctionDef(
                decorators=(
                    m.Decorator(
                        decorator=m.Call(
                            func=m.Name(value="pipeline"),
                            args=(
                                m.ZeroOrMore(),
                                m.Arg(
                                    keyword=m.Name(value="mode_defs"),
                                    value=m.List(
                                        elements=(
                                            m.Element(
                                                value=m.Call(
                                                    func=m.Name(value="ModeDefinition"),
                                                )
                                            ),
                                        )
                                    ),
                                ),
                                m.ZeroOrMore(),
                            ),
                        )
                    ),
                ),
            ),
        ):  # case where decorator is invoked a la @pipeline(..., mode_defs)
            updated_decorator = self._replace_single_mode_job_decorator(updated_node.decorators[0])
            if not updated_decorator:
                if RenameVariablesVisitor.CONTEXT_KEY in self.context.scratch:
                    self.context.scratch[RenameVariablesVisitor.CONTEXT_KEY].remove(
                        updated_node.name.value
                    )
                return updated_node
            self.required_imports.add("job")
            return updated_node.with_changes(decorators=[updated_decorator])

        return updated_node

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        if m.matches(updated_node, m.Call(func=m.Name(value="PipelineDefinition"))):
            args = []
            to_job_args = []
            for arg in updated_node.args:
                if m.matches(arg, m.Arg(keyword=m.Name(value="solid_defs"))):
                    args.append(
                        arg.with_deep_changes(cast(cst.Name, arg.keyword), value="node_defs")
                    )
                elif m.matches(arg, m.Arg(keyword=m.Name(value="mode_defs"))):
                    if not m.matches(
                        arg.value,
                        m.List(
                            elements=(m.Element(value=m.Call(func=m.Name(value="ModeDefinition"))),)
                        ),
                    ):
                        raise Exception("Could not handle PipelineDefinition")
                    mode_def_call = cast(cst.Call, cast(cst.List, arg.value).elements[0].value)
                    for mode_arg in mode_def_call.args:
                        if m.matches(mode_arg, m.Arg(keyword=m.Name(value="executor_defs"))):
                            raise Exception("Could not handle PipelineDefinition")
                        if (
                            m.matches(mode_arg, m.Arg(keyword=m.Name(value="name")))
                            or mode_arg.keyword is None
                        ):
                            continue
                        to_job_args.append(mode_arg)
                else:
                    args.append(arg)
            self.required_imports.add("GraphDefinition")
            return cst.Call(
                func=cst.Attribute(
                    value=updated_node.with_deep_changes(
                        updated_node.func, value="GraphDefinition"
                    ).with_changes(args=args),
                    attr=cst.Name(value="to_job"),
                ),
                args=to_job_args,
            )
        return updated_node

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        module_pipeline_refs_removed = self._instantiate_and_run(RenameVariablesVisitor, updated_node)  # type: ignore
        return AddImportsVisitor(
            context=self.context,
            imports=[
                ImportItem("dagster", required_import) for required_import in self.required_imports
            ],
        ).transform_module(module_pipeline_refs_removed)

    def _replace_single_mode_job_decorator(
        self, pipeline_decorator: cst.Decorator
    ) -> Optional[cst.Decorator]:
        mode_def_arg_idx = -1
        pipeline_decorator_args = cast(cst.Call, pipeline_decorator.decorator).args
        for i, arg in enumerate(cast(cst.Call, pipeline_decorator.decorator).args):
            if m.matches(arg, m.Arg(keyword=m.Name(value="mode_defs"))):
                mode_def_arg_idx = i
                break
        mode_def_arg = pipeline_decorator_args[mode_def_arg_idx]
        if not m.matches(
            mode_def_arg.value,
            m.List(
                elements=(
                    m.Element(
                        value=m.Call(
                            func=m.Name(value="ModeDefinition"),
                            args=(
                                m.ZeroOrMore(),
                                m.Arg(
                                    keyword=m.Name(value="resource_defs"),
                                ),
                                m.ZeroOrMore(),
                            ),
                        )
                    ),
                )
            ),
        ) or m.matches(
            mode_def_arg.value,
            m.List(
                elements=(
                    m.Element(
                        value=m.Call(
                            func=m.Name(value="ModeDefinition"),
                            args=(
                                m.ZeroOrMore(),
                                m.Arg(
                                    keyword=m.Name(value="logger_defs")
                                    | m.Name(value="executor_defs"),
                                ),
                                m.ZeroOrMore(),
                            ),
                        )
                    ),
                )
            ),
        ):
            return None
        mode_def_call = cast(cst.Call, cast(cst.List, mode_def_arg.value).elements[0].value)
        resource_defs_arg = -1
        for i, arg in enumerate(mode_def_call.args):
            if arg.keyword == "resource_defs":
                resource_defs_arg = i
                break

        job_decorator = pipeline_decorator.with_deep_changes(
            cast(cst.Call, pipeline_decorator.decorator).func, value="job"
        )
        return job_decorator.with_deep_changes(
            cast(cst.Call, job_decorator.decorator).args[mode_def_arg_idx],
            keyword=cst.Name(value="resource_defs"),
            value=mode_def_call.args[resource_defs_arg].value,
        )


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
        return orig_str.replace("pipeline", "job")
