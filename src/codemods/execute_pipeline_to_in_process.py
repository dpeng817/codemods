from typing import Union, Sequence, Set, Optional, Tuple, cast

import libcst as cst
from libcst.codemod import CodemodContext, VisitorBasedCodemodCommand
from libcst.codemod.visitors import AddImportsVisitor
from libcst.codemod._visitor import ContextAwareTransformer
import libcst.matchers as m
from libcst.codemod.visitors._imports import ImportItem


class ConvertExecutePipeline(VisitorBasedCodemodCommand):

    # Add a description so that future codemodders can see what this does.
    DESCRIPTION: str = (
        "Converts invocations of execute_pipeline to invocations of execute_in_process`."
    )

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        if m.matches(updated_node, m.Call(func=m.Name(value="execute_pipeline"))):
            execute_pipeline_call = updated_node
            execute_pipeline_other_args = execute_pipeline_call.args[1:]
            pipeline_name = cast(cst.Name, execute_pipeline_call.args[0].value)
            return execute_pipeline_call.with_changes(
                func=cst.Attribute(
                    value=pipeline_name, attr=cst.Name(value="execute_in_process"), dot=cst.Dot()
                ),
                args=execute_pipeline_other_args,
            )
        return updated_node
